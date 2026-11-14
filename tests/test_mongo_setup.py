"""Przygotowanie MongoDB: odczyt pliku .env i certyfikaty TLS."""
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from cryptography import x509

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import db
import make_certs


class MongoSetupTests(unittest.TestCase):
    def test_env_file_is_parsed(self):
        with tempfile.TemporaryDirectory() as folder:
            (Path(folder) / ".env").write_text("# komentarz\n\nMONGO_APP_USER = app\nATLAS_URI=mongodb+srv://u:p@x/?a=b\n",
                                               encoding="utf-8")
            with patch.object(db, "ROOT", Path(folder)):
                env = db.load_env()
        self.assertEqual(env, {"MONGO_APP_USER": "app", "ATLAS_URI": "mongodb+srv://u:p@x/?a=b"})

    def test_missing_env_file_gives_clear_error(self):
        with tempfile.TemporaryDirectory() as folder, patch.object(db, "ROOT", Path(folder)):
            with self.assertRaisesRegex(RuntimeError, ".env.example"):
                db.load_env()

    def test_server_certificate_is_signed_by_ca_for_our_hostnames(self):
        with tempfile.TemporaryDirectory() as folder, patch.object(make_certs, "TLS_DIR", Path(folder)), \
             patch.object(make_certs, "ROOT", Path(folder).parent), patch.object(sys, "argv", ["make_certs.py"]):
            make_certs.main()
            ca = x509.load_pem_x509_certificate((Path(folder) / "ca.pem").read_bytes())
            server = x509.load_pem_x509_certificate((Path(folder) / "server.pem").read_bytes())
        server.verify_directly_issued_by(ca)  # wyjątek, jeśli podpis się nie zgadza
        names = server.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
        self.assertEqual(names.get_values_for_type(x509.DNSName), ["localhost", "mongodb"])
        self.assertFalse(server.extensions.get_extension_for_class(x509.BasicConstraints).value.ca)


if __name__ == "__main__":
    unittest.main()
