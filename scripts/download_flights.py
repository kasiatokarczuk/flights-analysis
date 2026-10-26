"""Loty: 21 miesiecy lotow, styczen 2024 - wrzesien 2025. Uruchom ponownie, aby wznowic."""
import argparse
import contextlib
import csv
import hashlib
import http.client
import json
import msvcrt
import shutil
import sys
import time
import urllib.error
import urllib.request
import zipfile
import zlib
from datetime import date
from pathlib import Path

OUTPUT = Path(__file__).resolve().parents[1] / "data"
CATALOG = "https://transtats.bts.gov/PREZIP/"
DATASET = "Reporting Carrier On-Time Performance (1987-present)"
MONTHS = [f"2024-{m:02d}" for m in range(1, 13)] + [f"2025-{m:02d}" for m in range(1, 10)]
GB = 1_000_000_000
MAX_CSV_BYTES = 6 * GB


def checksum(path):
    """Suma kontrolna pozwala sprawdzic, czy gotowy plik nie zostal zmieniony."""
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def save_manifest(root, manifest):
    temporary = root / "manifest.json.tmp"
    temporary.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    temporary.replace(root / "manifest.json")


@contextlib.contextmanager
def directory_lock(root):
    """Windows zwalnia blokade po zamknieciu pliku, takze po przerwaniu programu."""
    with (root / ".download.lock").open("a+b") as lock:
        if lock.tell() == 0:
            lock.write(b"0")
            lock.flush()
        lock.seek(0)
        try:
            msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError as error:
            raise RuntimeError("Pliki danych sa juz uzywane przez inny skrypt.") from error
        yield


def download_zip(url, path):
    """Czytaj po 1 MB; po bledzie polaczenia ponow najwyzej dwukrotnie."""
    temporary = path.with_suffix(".zip.part")
    for attempt in range(3):
        try:
            with urllib.request.urlopen(url, timeout=90) as source, temporary.open("wb") as target:
                expected = int(source.headers.get("Content-Length", "0"))
                size = 0
                while chunk := source.read(1024 * 1024):
                    size += len(chunk)
                    if size > 100_000_000:
                        raise ValueError("ZIP przekracza limit 100 MB.")
                    target.write(chunk)
                    if size % (5 * 1024 * 1024) == 0:
                        print(f"  Pobrano {size / 1_000_000:.1f} MB ZIP", flush=True)
                if not size or (expected and size != expected):
                    raise http.client.IncompleteRead(b"")
            temporary.replace(path)
            return
        except (urllib.error.URLError, TimeoutError, ConnectionError, http.client.IncompleteRead) as error:
            if isinstance(error, urllib.error.HTTPError) and error.code not in (429, 500, 502, 503, 504):
                raise
            if attempt == 2:
                raise RuntimeError(f"Nie udalo sie pobrac ZIP po 3 probach: {error}") from error
            retry_after = getattr(error, "headers", {}).get("Retry-After", "0")
            delay = max(15 * (attempt + 1), int(retry_after) if retry_after.isdigit() else 0)
            print(f"  Blad polaczenia: {error}. Ponowienie za {delay} s.", flush=True)
            time.sleep(delay)


def extract_csv(archive, temporary, label, columns):
    """Rozpakuj oryginalny CSV i sprawdz jego strukture; ZipFile weryfikuje CRC."""
    with zipfile.ZipFile(archive) as zipped:
        files = [f for f in zipped.infolist() if f.filename.endswith(".csv") and not f.filename.startswith("__MACOSX/")]
        if len(files) != 1:
            raise ValueError("Oczekiwano jednego CSV w ZIP.")
        info = files[0]
        existing = sum(p.stat().st_size for p in temporary.parent.glob("*.csv"))
        if info.file_size > GB or existing + info.file_size > MAX_CSV_BYTES:
            raise ValueError("Przekroczony limit rozmiaru CSV: 1 GB na miesiac, 6 GB lacznie.")
        if shutil.disk_usage(temporary.parent).free < 15 * GB + info.file_size:
            raise RuntimeError("Za malo wolnego miejsca; zostawiamy 15 GB zapasu.")
        with zipped.open(info) as source, temporary.open("wb") as target:
            shutil.copyfileobj(source, target, 1024 * 1024)
    with temporary.open(encoding="utf-8-sig", newline="") as stream:
        reader = csv.reader(stream, strict=True)
        header = next(reader, [])
        required = {"FlightDate", "Reporting_Airline", "Origin", "Dest", "CRSDepTime", "ArrDelay", "Cancelled", "Diverted"}
        if not required.issubset(header) or len(header) != len(set(header)) or (columns and header != columns):
            raise ValueError("Nieoczekiwany schemat CSV lotow.")
        position = header.index("FlightDate")
        records, previous_line = 0, reader.line_num
        for row in reader:
            if len(row) != len(header) or reader.line_num != previous_line + 1:
                raise ValueError("Bledna liczba pol lub wielowierszowy rekord CSV.")
            if date.fromisoformat(row[position]).strftime("%Y-%m") != label:
                raise ValueError("Lot spoza wybranego miesiaca.")
            records, previous_line = records + 1, reader.line_num
    if not records or temporary.stat().st_size != info.file_size:
        raise ValueError("Pusty lub niekompletny CSV.")
    return header, {"file": label + ".csv", "records": records, "bytes": info.file_size,
                    "sha256": checksum(temporary), "zip_bytes": archive.stat().st_size}


def download_month(root, label, manifest):
    final = root / (label + ".csv")
    temporary = root / (label + ".csv.part")
    archive = root / (label + ".zip")
    year, number = label.split("-")
    url = CATALOG + f"On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{int(number)}.zip"
    saved = manifest["files"].get(label)
    if saved:
        candidate = final if final.exists() else temporary
        if candidate.stat().st_size != saved["bytes"] or checksum(candidate) != saved["sha256"]:
            raise ValueError(f"CSV {label} nie zgadza sie z manifestem; zachowano plik.")
        if candidate == temporary:
            temporary.replace(final)
        print(f"{label}: sprawdzono, bez ponownego pobierania.", flush=True)
    else:
        if final.exists():
            raise ValueError(f"CSV {label} istnieje bez wpisu w manifescie; nie nadpisano go.")
        if shutil.disk_usage(root).free < 15 * GB + 100_000_000:
            raise RuntimeError("Za malo wolnego miejsca; zostawiamy 15 GB zapasu.")
        print(f"{label}: pobieranie i sprawdzanie...", flush=True)
        if not archive.exists():
            download_zip(url, archive)
        try:
            columns, entry = extract_csv(archive, temporary, label, manifest["columns"])
        except (zipfile.BadZipFile, zlib.error, EOFError):
            archive.unlink(missing_ok=True)
            raise ValueError("Uszkodzony ZIP usunieto. Wznow ta sama komende.")
        entry["url"] = url
        manifest["columns"], manifest["files"][label] = columns, entry
        # Najpierw manifest: po przerwaniu mozemy odzyskac gotowy plik .csv.part.
        save_manifest(root, manifest)
        temporary.replace(final)
        print(f"{label}: {entry['records']} lotow, {entry['bytes'] / GB:.3f} GB CSV.", flush=True)
    archive.unlink(missing_ok=True)
    archive.with_suffix(".zip.part").unlink(missing_ok=True)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Pokaz plan bez pobierania.")
    args = parser.parse_args(argv)
    print(f"Plan: {len(MONTHS)} miesiecy, 2024-01 - 2025-09, okolo 5,57 GB CSV.", flush=True)
    if args.dry_run:
        return
    OUTPUT.mkdir(parents=True, exist_ok=True)
    with directory_lock(OUTPUT):
        path = OUTPUT / "manifest.json"
        manifest = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {
            "dataset": DATASET, "catalog_url": CATALOG, "format": "csv", "columns": [], "files": {}}
        if manifest.get("dataset") != DATASET or manifest.get("format") != "csv":
            raise ValueError("To nie jest manifest danych lotow CSV.")
        for label in MONTHS:
            download_month(OUTPUT, label, manifest)
        entries = [manifest["files"][label] for label in MONTHS]
        total = sum(e["bytes"] for e in entries)
        print(f"GOTOWE: {len(entries)} miesiecy, {total / GB:.3f} GB CSV w data/.")
        if total < 5.5 * GB:
            print("Uwaga: wynik jest mniejszy niz docelowe 5,5 GB.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nPrzerwano. Uruchom ta sama komende, aby wznowic.", file=sys.stderr)
        sys.exit(130)
    except (OSError, ValueError, KeyError, RuntimeError, csv.Error) as error:
        print(f"BLAD: {error}\nGotowe miesiace zachowano. Uruchom ta sama komende.", file=sys.stderr)
        sys.exit(1)
