"""Offline: oryginalny CSV, walidacja ZIP i bezpieczne wznowienie miesiecy."""
import contextlib
import csv
import importlib.util
import io
import json
import tempfile
import unittest
import urllib.error
import zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("download_flights", ROOT / "scripts/download_flights.py")
downloader = importlib.util.module_from_spec(spec)
spec.loader.exec_module(downloader)
FIELDS = ["FlightDate", "Reporting_Airline", "Origin", "Dest", "CRSDepTime",
          "ArrDelay", "Cancelled", "Diverted", "OriginCityName", ""]


def archive_bytes(label="2024-01", *, fields=None, date_value=None, name="flights.csv"):
    text = io.StringIO(newline="")
    writer = csv.writer(text, lineterminator="\r\n")
    writer.writerow(FIELDS if fields is None else fields)
    writer.writerow([date_value or label + "-01", "AA", "JFK", "LAX", "0659",
                     "-7.00", "0.00", "0.00", "New York, NY", ""])
    raw = text.getvalue().encode("utf-8")
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zipped:
        zipped.writestr(name, raw)
    return buffer.getvalue(), raw


class FlightsTests(unittest.TestCase):
    def setUp(self):
        self.parent = ROOT / "tmp"
        self.parent.mkdir(exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(prefix="flights-test-", dir=self.parent)
        self.root = Path(self.temp.name).resolve()
        self.addCleanup(self.clean)
        self.manifest = {"dataset": downloader.DATASET, "catalog_url": downloader.CATALOG,
                         "format": "csv", "columns": [], "files": {}}
        output = contextlib.redirect_stdout(io.StringIO())
        output.__enter__()
        self.addCleanup(output.__exit__, None, None, None)

    def clean(self):
        assert self.root.is_relative_to((ROOT / "tmp").resolve())
        self.temp.cleanup()
        if self.parent.exists() and not any(self.parent.iterdir()):
            self.parent.rmdir()

    def plant_zip(self, label="2024-01", **kwargs):
        zipped, raw = archive_bytes(label, **kwargs)
        (self.root / (label + ".zip")).write_bytes(zipped)
        return raw

    def run_month(self, label="2024-01"):
        return downloader.download_month(self.root, label, self.manifest)

    def test_original_csv_bytes_crc_and_cached_repeat(self):
        raw = self.plant_zip()
        with patch.object(downloader, "download_zip", side_effect=AssertionError("No network")):
            self.run_month()
            self.run_month()
        final = self.root / "2024-01.csv"
        self.assertEqual(final.read_bytes(), raw)
        saved = json.loads((self.root / "manifest.json").read_text())
        self.assertEqual(saved["files"]["2024-01"]["records"], 1)
        self.assertEqual(saved["files"]["2024-01"]["sha256"], downloader.checksum(final))
        self.assertFalse(list(self.root.glob("*.zip")))
        self.assertFalse(list(self.root.glob("*.part")))

    def test_interruption_after_manifest_before_promotion_recovers_without_network(self):
        raw = self.plant_zip()
        replace = Path.replace
        def fail_csv(path, target):
            if path.name.endswith(".csv.part"):
                raise PermissionError("Simulated editor lock")
            return replace(path, target)
        with patch.object(Path, "replace", autospec=True, side_effect=fail_csv):
            with self.assertRaises(PermissionError):
                self.run_month()
        self.manifest = json.loads((self.root / "manifest.json").read_text())
        with patch.object(downloader, "download_zip", side_effect=AssertionError("No network")):
            self.run_month()
        self.assertEqual((self.root / "2024-01.csv").read_bytes(), raw)

    def test_failed_manifest_write_keeps_csv_uncommitted_and_can_resume(self):
        raw = self.plant_zip()
        with patch.object(downloader, "save_manifest", side_effect=OSError("Interrupted")):
            with self.assertRaises(OSError):
                self.run_month()
        self.assertFalse((self.root / "2024-01.csv").exists())
        self.manifest["files"].clear()
        self.manifest["columns"] = []
        self.run_month()
        self.assertEqual((self.root / "2024-01.csv").read_bytes(), raw)

    def test_corrupted_completed_csv_is_preserved_and_rejected(self):
        self.plant_zip()
        self.run_month()
        final = self.root / "2024-01.csv"
        final.write_bytes(b"corrupted data")
        with self.assertRaisesRegex(ValueError, "manifestem"):
            self.run_month()
        self.assertEqual(final.read_bytes(), b"corrupted data")

    def test_untracked_csv_is_not_overwritten(self):
        final = self.root / "2024-01.csv"
        final.write_bytes(b"original")
        with self.assertRaisesRegex(ValueError, "bez wpisu"):
            self.run_month()
        self.assertEqual(final.read_bytes(), b"original")

    def test_invalid_month_schema_and_multiline_are_not_committed(self):
        for kwargs in ({"date_value": "2024-02-01"}, {"fields": FIELDS[:-3]},
                       {"date_value": "2024-01-01\n2024-01-02"}, {"date_value": "2024-01-99"}):
            with self.subTest(kwargs=kwargs):
                self.plant_zip(**kwargs)
                with self.assertRaises(ValueError):
                    self.run_month()
                self.assertFalse((self.root / "2024-01.csv").exists())
                self.assertFalse(self.manifest["files"])

    def test_header_change_between_months_is_rejected(self):
        self.plant_zip()
        self.run_month()
        fields = FIELDS.copy()
        fields[-2] = "DifferentColumn"
        self.plant_zip("2024-02", fields=fields)
        with self.assertRaisesRegex(ValueError, "schemat"):
            self.run_month("2024-02")
        self.assertNotIn("2024-02", self.manifest["files"])

    def test_archive_names_cannot_write_outside_output(self):
        raw = self.plant_zip(name="../../unexpected.csv")
        self.run_month()
        self.assertEqual((self.root / "2024-01.csv").read_bytes(), raw)
        self.assertFalse((ROOT / "unexpected.csv").exists())

    def test_bad_zip_is_removed_without_touching_completed_csv(self):
        self.plant_zip()
        self.run_month()
        first = (self.root / "2024-01.csv").read_bytes()
        (self.root / "2024-02.zip").write_bytes(b"not a zip")
        with self.assertRaisesRegex(ValueError, "Uszkodzony ZIP"):
            self.run_month("2024-02")
        self.assertFalse((self.root / "2024-02.zip").exists())
        self.assertEqual((self.root / "2024-01.csv").read_bytes(), first)

    def test_low_disk_space_stops_before_download(self):
        with patch.object(downloader.shutil, "disk_usage", return_value=SimpleNamespace(free=0)):
            with patch.object(downloader, "download_zip") as request:
                with self.assertRaisesRegex(RuntimeError, "malo wolnego"):
                    self.run_month()
                request.assert_not_called()

    def test_crc_failure_discards_archive_without_committing_csv(self):
        _, raw = archive_bytes()
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_STORED) as zipped:
            zipped.writestr("flights.csv", raw)
        damaged = buffer.getvalue().replace(b"-7.00", b"-8.00", 1)
        (self.root / "2024-01.zip").write_bytes(damaged)
        with self.assertRaisesRegex(ValueError, "Uszkodzony ZIP"):
            self.run_month()
        self.assertFalse((self.root / "2024-01.csv").exists())
        self.assertFalse((self.root / "2024-01.zip").exists())
        self.assertFalse(self.manifest["files"])

    def test_csv_budget_stops_before_extracting(self):
        self.plant_zip()
        with patch.object(downloader, "MAX_CSV_BYTES", 1):
            with self.assertRaisesRegex(ValueError, "limit"):
                self.run_month()
        self.assertFalse((self.root / "2024-01.csv.part").exists())

    def test_download_restarts_partial_zip_after_connection_failure(self):
        zipped, _ = archive_bytes()
        response = io.BytesIO(zipped)
        response.status, response.headers = 200, {"Content-Length": str(len(zipped))}
        (self.root / "2024-01.zip.part").write_bytes(b"old partial download")
        with patch.object(downloader.urllib.request, "urlopen", side_effect=[urllib.error.URLError("outage"), response]) as request:
            with patch.object(downloader.time, "sleep"):
                downloader.download_zip("https://example.test/month.zip", self.root / "2024-01.zip")
        self.assertEqual(request.call_count, 2)
        self.assertEqual((self.root / "2024-01.zip").read_bytes(), zipped)

    def test_incomplete_http_body_never_becomes_completed_zip(self):
        def incomplete(*args, **kwargs):
            response = io.BytesIO(b"short")
            response.status, response.headers = 200, {"Content-Length": "100"}
            return response
        with patch.object(downloader.urllib.request, "urlopen", side_effect=incomplete):
            with patch.object(downloader.time, "sleep"):
                with self.assertRaisesRegex(RuntimeError, "3 probach"):
                    downloader.download_zip("https://example.test", self.root / "2024-01.zip")
        self.assertFalse((self.root / "2024-01.zip").exists())

    def test_permanent_http_error_is_not_retried(self):
        error = urllib.error.HTTPError("https://example.test", 404, "Not found", {}, None)
        with patch.object(downloader.urllib.request, "urlopen", side_effect=error) as request:
            with self.assertRaises(urllib.error.HTTPError):
                downloader.download_zip("https://example.test", self.root / "2024-01.zip")
        self.assertEqual(request.call_count, 1)

    def test_second_importer_is_blocked(self):
        with downloader.directory_lock(self.root):
            with self.assertRaisesRegex(RuntimeError, "Pliki danych"):
                with downloader.directory_lock(self.root):
                    self.fail("Acquired a second lock")

    def test_default_range_and_dry_run(self):
        self.assertEqual(len(downloader.MONTHS), 21)
        self.assertEqual((downloader.MONTHS[0], downloader.MONTHS[-1]), ("2024-01", "2025-09"))
        absent = self.root / "not-created"
        with patch.object(downloader, "OUTPUT", absent), patch.object(downloader, "download_month") as request:
            downloader.main(["--dry-run"])
            request.assert_not_called()
        self.assertFalse(absent.exists())

    def test_main_downloads_selected_months_and_reuses_manifest(self):
        self.plant_zip()
        self.plant_zip("2024-02")
        with patch.object(downloader, "OUTPUT", self.root), patch.object(downloader, "MONTHS", ["2024-01", "2024-02"]):
            downloader.main([])
            with patch.object(downloader, "download_zip", side_effect=AssertionError("No network")):
                downloader.main([])
        manifest = json.loads((self.root / "manifest.json").read_text())
        self.assertEqual(list(manifest["files"]), ["2024-01", "2024-02"])
        self.assertFalse((self.root / "2024-03.csv").exists())


if __name__ == "__main__":
    unittest.main()
