"""Kopiuje 21 plików CSV i manifest do HDFS (/flights/raw/).

Każda kopia jest sprawdzana sumą SHA-256. Pliki, które już są w HDFS, zostają pominięte.
"""
import hashlib
import json
import shutil
import subprocess
import sys

from download_flights import OUTPUT, DATASET, MONTHS, checksum, directory_lock

ROOT = OUTPUT.parent
HDFS_DIR = "/flights/raw"


def docker(*command, stdin=None, check=True):
    """Wykonuje polecenie w kontenerze namenode."""
    result = subprocess.run(
        ["docker", "compose", "-f", str(ROOT / "compose.yaml"), "exec", "-T", "namenode", *command],
        cwd=ROOT, stdin=stdin, capture_output=True, text=True,
    )
    if check and result.returncode:
        raise RuntimeError(result.stderr.strip() or "Polecenie Docker zakończyło się błędem.")
    return result


def hdfs_hash(path):
    # Sumę liczymy w kontenerze, więc do Windows wraca tylko wynik, a nie cały plik CSV.
    result = docker("bash", "-o", "pipefail", "-c", 'hdfs dfs -cat "$1" | sha256sum', "_", path)
    return result.stdout.split()[0]


def copy_file(local, remote, expected_hash, expected_bytes):
    """Kopiuje plik do HDFS pod nazwą tymczasową, a po sprawdzeniu sumy zmienia ją na docelową."""
    if local.stat().st_size != expected_bytes or checksum(local) != expected_hash:
        raise ValueError(f"Lokalny plik {local.name} nie zgadza się z manifestem.")
    if docker("hdfs", "dfs", "-test", "-e", remote, check=False).returncode == 0:
        if hdfs_hash(remote) != expected_hash:
            raise ValueError(f"Plik {remote} w HDFS ma inną zawartość. Pliku nie nadpisano.")
        print(f"{local.name}: już jest w HDFS (suma zgodna).")
        return
    if shutil.disk_usage(ROOT).free < expected_bytes + 15_000_000_000:
        raise RuntimeError("Za mało miejsca na dysku na kopię w HDFS (potrzebne 15 GB zapasu).")
    parent, name = remote.rsplit("/", 1)
    temporary = parent + "/." + name + ".uploading"
    print(f"{local.name}: kopiowanie do HDFS...")
    # Plik trafia z dysku prosto do HDFS, bez kopii pośredniej w kontenerze.
    with local.open("rb") as source:
        docker("hdfs", "dfs", "-put", "-f", "-", temporary, stdin=source)
    if hdfs_hash(temporary) != expected_hash:
        raise ValueError(f"Kopia {local.name} w HDFS ma inną sumę SHA-256 niż oryginał.")
    docker("hdfs", "dfs", "-mv", temporary, remote)
    print(f"{local.name}: skopiowano.")


def main():
    with directory_lock(OUTPUT):
        path = OUTPUT / "manifest.json"
        raw_manifest = path.read_bytes()
        manifest = json.loads(raw_manifest)
        if manifest.get("dataset") != DATASET or manifest.get("format") != "csv" or sorted(manifest["files"]) != MONTHS:
            raise ValueError("Manifest nie opisuje 21 miesięcy danych o lotach.")
        docker("hdfs", "dfs", "-mkdir", "-p", HDFS_DIR)
        total = 0
        for label in MONTHS:
            entry = manifest["files"][label]
            name = label + ".csv"
            if entry["file"] != name:
                raise ValueError(f"Nieoczekiwana nazwa pliku w manifeście: {entry['file']}.")
            copy_file(OUTPUT / name, HDFS_DIR + "/" + name, entry["sha256"], entry["bytes"])
            total += entry["bytes"]
        # Manifest wgrywamy na końcu, gdy wszystkie pliki CSV są już sprawdzone.
        copy_file(path, "/flights/manifest.json", hashlib.sha256(raw_manifest).hexdigest(), len(raw_manifest))
        print(f"GOTOWE: {len(MONTHS)} plików ({total / 1_000_000_000:.2f} GB) w HDFS {HDFS_DIR}.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit("\nPrzerwano. Uruchom skrypt ponownie - gotowe pliki zostaną pominięte.")
    except (OSError, ValueError, RuntimeError) as error:
        sys.exit(f"BŁĄD: {error}")
