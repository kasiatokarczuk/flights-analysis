"""Skopiuj 21 CSV do HDFS. Gotowe pliki sprawdz i pomin. Biblioteka standardowa."""
import hashlib
import json
import shutil
import subprocess
import sys

from download_flights import OUTPUT, DATASET, MONTHS, checksum, directory_lock

ROOT = OUTPUT.parent
HDFS_DIR = "/flights/raw"


def docker(*command, stdin=None, check=True):
    result = subprocess.run(
        ["docker", "compose", "-f", str(ROOT / "compose.yaml"), "exec", "-T", "namenode", *command],
        cwd=ROOT, stdin=stdin, capture_output=True, text=True,
    )
    if check and result.returncode:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip() or "Blad polecenia Docker/HDFS.")
    return result


def hdfs_hash(path):
    # Liczymy sume w kontenerze. Do Windows wraca tylko krotki wynik, nie caly CSV.
    result = docker("bash", "-o", "pipefail", "-c", 'hdfs dfs -cat "$1" | sha256sum', "_", path)
    return result.stdout.split()[0]


def copy_file(local, remote, expected_hash, expected_bytes):
    if local.stat().st_size != expected_bytes or checksum(local) != expected_hash:
        raise ValueError(f"Lokalny plik nie zgadza sie z manifestem: {local.name}")
    if docker("hdfs", "dfs", "-test", "-e", remote, check=False).returncode == 0:
        if hdfs_hash(remote) != expected_hash:
            raise ValueError(f"Inna zawartosc w HDFS: {remote}. Pliku nie nadpisano.")
        print(f"{local.name}: juz w HDFS, zgodna suma kontrolna.", flush=True)
        return
    if shutil.disk_usage(ROOT).free < expected_bytes + 15_000_000_000:
        raise RuntimeError("Za malo miejsca na dodatkowa kopie HDFS i 15 GB zapasu.")
    parent, name = remote.rsplit("/", 1)
    temporary = parent + "/." + name + ".uploading"
    print(f"{local.name}: kopiowanie do HDFS...", flush=True)
    # Strumien z dysku trafia wprost do HDFS, bez kopii CSV w /tmp kontenera.
    with local.open("rb") as source:
        docker("hdfs", "dfs", "-put", "-f", "-", temporary, stdin=source)
    if hdfs_hash(temporary) != expected_hash:
        raise ValueError(f"Kopia HDFS {local.name} nie przeszla kontroli SHA-256.")
    docker("hdfs", "dfs", "-mv", temporary, remote)
    print(f"{local.name}: GOTOWE, kopia identyczna.", flush=True)


def main():
    with directory_lock(OUTPUT):
        path = OUTPUT / "manifest.json"
        raw_manifest = path.read_bytes()
        manifest = json.loads(raw_manifest)
        if manifest.get("dataset") != DATASET or manifest.get("format") != "csv" or sorted(manifest["files"]) != MONTHS:
            raise ValueError("Manifest nie opisuje wybranych 21 miesiecy lotow CSV.")
        docker("hdfs", "dfs", "-mkdir", "-p", HDFS_DIR)
        total = 0
        for label in MONTHS:
            entry = manifest["files"][label]
            name = label + ".csv"
            if entry["file"] != name:
                raise ValueError("Nieoczekiwana nazwa CSV w manifescie.")
            copy_file(OUTPUT / name, HDFS_DIR + "/" + name, entry["sha256"], entry["bytes"])
            total += entry["bytes"]
        # Manifest publikujemy dopiero po sprawdzeniu wszystkich CSV.
        copy_file(path, "/flights/manifest.json", hashlib.sha256(raw_manifest).hexdigest(), len(raw_manifest))
        print(f"GOTOWE: 21 CSV, {total / 1_000_000_000:.3f} GB w HDFS {HDFS_DIR}.")
        print("Lokalne CSV pozostaly w data/.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nPrzerwano. Ta sama komenda sprawdzi gotowe pliki i wznowi pozostale.", file=sys.stderr)
        sys.exit(130)
    except (OSError, ValueError, KeyError, RuntimeError, IndexError) as error:
        print(f"BLAD: {error}\nGotowe CSV zachowano. Mozesz wznowic ta sama komende.", file=sys.stderr)
        sys.exit(1)
