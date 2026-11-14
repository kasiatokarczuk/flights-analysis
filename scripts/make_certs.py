"""Tworzy certyfikaty TLS dla lokalnej bazy MongoDB.

    python scripts/make_certs.py          # utwórz, jeśli jeszcze nie ma
    python scripts/make_certs.py --force  # utwórz od nowa

Powstaje własny urząd certyfikacji (CA) i podpisany przez niego certyfikat serwera.
Klient (skrypty projektu, MongoDB Compass) ufa serwerowi, bo zna certyfikat CA.
Pliki trafiają do secrets/tls/ (katalog jest w .gitignore):
    ca.pem      - certyfikat CA, podajemy go klientom,
    ca.key      - klucz prywatny CA (potrzebny tylko do podpisania certyfikatu serwera),
    server.pem  - certyfikat serwera razem z jego kluczem (tego formatu wymaga mongod).
"""
import argparse
import ipaddress
import sys
from datetime import datetime, timedelta, timezone

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from common import ROOT

TLS_DIR = ROOT / "secrets" / "tls"
# Nazwy, pod którymi łączymy się z bazą: z Windows (localhost) i z innych kontenerów (mongodb).
HOSTNAMES = ["localhost", "mongodb"]
VALID_DAYS = 365


def new_key():
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def pem(key):
    return key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.TraditionalOpenSSL,
                             serialization.NoEncryption())


def certificate(subject, issuer, public_key, signing_key, is_ca, names=()):
    """Buduje i podpisuje certyfikat X.509."""
    now = datetime.now(timezone.utc)
    builder = (x509.CertificateBuilder()
               .subject_name(subject).issuer_name(issuer).public_key(public_key)
               .serial_number(x509.random_serial_number())
               .not_valid_before(now - timedelta(minutes=5))
               .not_valid_after(now + timedelta(days=VALID_DAYS))
               .add_extension(x509.BasicConstraints(ca=is_ca, path_length=None), critical=True))
    if names:
        # Klient sprawdza, czy nazwa, pod którą się łączy, jest na tej liście.
        alt_names = [x509.DNSName(name) for name in names] + [x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]
        builder = builder.add_extension(x509.SubjectAlternativeName(alt_names), critical=False)
    return builder.sign(signing_key, hashes.SHA256())


def main():
    parser = argparse.ArgumentParser(description="Certyfikaty TLS dla lokalnej MongoDB.")
    parser.add_argument("--force", action="store_true", help="utwórz certyfikaty od nowa")
    args = parser.parse_args()
    if (TLS_DIR / "server.pem").exists() and not args.force:
        print(f"Certyfikaty już są w {TLS_DIR.relative_to(ROOT)}. Aby utworzyć nowe, użyj --force.")
        return

    ca_key, server_key = new_key(), new_key()
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Flights Project Local CA")])
    server_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
    ca_cert = certificate(ca_name, ca_name, ca_key.public_key(), ca_key, is_ca=True)
    server_cert = certificate(server_name, ca_name, server_key.public_key(), ca_key,
                              is_ca=False, names=HOSTNAMES)

    TLS_DIR.mkdir(parents=True, exist_ok=True)
    (TLS_DIR / "ca.pem").write_bytes(ca_cert.public_bytes(serialization.Encoding.PEM))
    (TLS_DIR / "ca.key").write_bytes(pem(ca_key))
    (TLS_DIR / "server.pem").write_bytes(server_cert.public_bytes(serialization.Encoding.PEM) + pem(server_key))
    print(f"GOTOWE: certyfikaty w {TLS_DIR.relative_to(ROOT)} (ważne {VALID_DAYS} dni).")


if __name__ == "__main__":
    try:
        main()
    except OSError as error:
        sys.exit(f"BŁĄD: {error}")
