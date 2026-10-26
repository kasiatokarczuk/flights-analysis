"""Offline: transfer pliku, sumy kontrolne, przerwanie i ponowny import bez duplikatow."""
import contextlib
import hashlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import import_hdfs as importer


class FakeHdfs:
    def __init__(self):
        self.files, self.uploads = {}, []
        self.interrupt = False
        self.corrupt_name = None

    def run(self, *command, stdin=None, check=True):
        result = SimpleNamespace(returncode=0, stdout="", stderr="")
        if command[0] == "bash":
            result.stdout = hashlib.sha256(self.files[command[-1]]).hexdigest() + "  -\n"
            return result
        operation = command[2]
        if operation == "-test":
            result.returncode = 0 if command[-1] in self.files else 1
        elif operation == "-put":
            name, data = command[-1], stdin.read()
            self.uploads.append(name)
            if self.interrupt:
                self.interrupt = False
                self.files[name] = data[:2]
                raise RuntimeError("Connection interrupted")
            self.files[name] = data + b"damaged" if self.corrupt_name == name else data
        elif operation == "-mv":
            source, target = command[-2:]
            if target in self.files:
                raise RuntimeError("Destination already exists")
            self.files[target] = self.files.pop(source)
        elif operation != "-mkdir":
            raise AssertionError("Unexpected HDFS operation")
        return result


class HdfsImportTests(unittest.TestCase):
    def setUp(self):
        self.parent = ROOT / "tmp"
        self.parent.mkdir(exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(prefix="hdfs-test-", dir=self.parent)
        self.root = Path(self.temp.name).resolve()
        self.addCleanup(self.clean)
        self.local = self.root / "2024-01.csv"
        self.data = b"FlightDate,Origin\n2024-01-01,JFK\n"
        self.local.write_bytes(self.data)
        self.digest = hashlib.sha256(self.data).hexdigest()
        self.remote = "/flights/raw/2024-01.csv"
        self.backend = FakeHdfs()
        patch.object(importer, "docker", side_effect=self.backend.run).start()
        self.addCleanup(patch.stopall)
        output = contextlib.redirect_stdout(io.StringIO())
        output.__enter__()
        self.addCleanup(output.__exit__, None, None, None)

    def clean(self):
        assert self.root.is_relative_to((ROOT / "tmp").resolve())
        self.temp.cleanup()
        if self.parent.exists() and not any(self.parent.iterdir()):
            self.parent.rmdir()

    def copy(self):
        importer.copy_file(self.local, self.remote, self.digest, len(self.data))

    def test_copy_preserves_bytes_and_repeat_does_not_upload_again(self):
        self.copy()
        self.copy()
        self.assertEqual(self.backend.files, {self.remote: self.data})
        self.assertEqual(len(self.backend.uploads), 1)
        self.assertEqual(self.local.read_bytes(), self.data)

    def test_modified_local_csv_is_rejected_before_upload(self):
        self.local.write_bytes(b"modified")
        with self.assertRaisesRegex(ValueError, "Lokalny plik"):
            self.copy()
        self.assertFalse(self.backend.uploads)

    def test_different_remote_file_is_preserved(self):
        self.backend.files[self.remote] = b"other dataset"
        with self.assertRaisesRegex(ValueError, "nie nadpisano"):
            self.copy()
        self.assertEqual(self.backend.files[self.remote], b"other dataset")
        self.assertFalse(self.backend.uploads)

    def test_interrupted_copy_resumes_from_file_boundary(self):
        self.backend.interrupt = True
        with self.assertRaisesRegex(RuntimeError, "interrupted"):
            self.copy()
        self.assertNotIn(self.remote, self.backend.files)
        self.copy()
        self.assertEqual(self.backend.files, {self.remote: self.data})

    def test_bad_remote_hash_never_promotes_csv(self):
        self.backend.corrupt_name = "/flights/raw/.2024-01.csv.uploading"
        with self.assertRaisesRegex(ValueError, "SHA-256"):
            self.copy()
        self.assertNotIn(self.remote, self.backend.files)

    def test_low_disk_space_stops_new_copy_but_allows_cached_file(self):
        with patch.object(importer.shutil, "disk_usage", return_value=SimpleNamespace(free=0)):
            with self.assertRaisesRegex(RuntimeError, "miejsca"):
                self.copy()
            self.backend.files[self.remote] = self.data
            self.copy()
        self.assertFalse(self.backend.uploads)

    def test_manifest_is_published_only_after_all_csv_are_verified(self):
        second = self.root / "2024-02.csv"
        second.write_bytes(self.data)
        entry = {"bytes": len(self.data), "sha256": self.digest}
        manifest = {"dataset": importer.DATASET, "format": "csv", "files": {
            "2024-01": {**entry, "file": self.local.name},
            "2024-02": {**entry, "file": second.name}}}
        raw = json.dumps(manifest).encode()
        (self.root / "manifest.json").write_bytes(raw)
        self.backend.corrupt_name = "/flights/raw/.2024-02.csv.uploading"
        with patch.object(importer, "OUTPUT", self.root), patch.object(importer, "MONTHS", ["2024-01", "2024-02"]):
            with self.assertRaises(ValueError):
                importer.main()
            self.assertNotIn("/flights/manifest.json", self.backend.files)
            self.backend.corrupt_name = None
            importer.main()
        self.assertEqual(self.backend.files["/flights/manifest.json"], raw)
        self.assertEqual(self.backend.uploads.count("/flights/raw/.2024-01.csv.uploading"), 1)

    def test_docker_failure_exposes_reason(self):
        # Test rzeczywistego wrappera zamiast atrapionego backendu.
        patch.stopall()
        failure = SimpleNamespace(returncode=1, stderr="NameNode unavailable", stdout="")
        with patch.object(importer.subprocess, "run", return_value=failure):
            with self.assertRaisesRegex(RuntimeError, "NameNode unavailable"):
                importer.docker("hdfs", "dfs", "-ls", "/")


if __name__ == "__main__":
    unittest.main()
