"""Pobiera dane o lotach w USA (styczeń 2024 – wrzesień 2025) ze strony BTS.

Ponowne uruchomienie pomija gotowe miesiące i wznawia przerwane pobieranie.
"""
import argparse
import contextlib
import csv
import hashlib
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
    """Zwraca sumę SHA-256 pliku. Pozwala sprawdzić, czy gotowy plik nie został zmieniony."""
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def save_manifest(root, manifest):
    """Zapisuje manifest przez plik tymczasowy, żeby przerwanie nie zostawiło uszkodzonego JSON-a."""
    temporary = root / "manifest.json.tmp"
    temporary.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    temporary.replace(root / "manifest.json")


@contextlib.contextmanager
def directory_lock(root):
    """Blokuje folder danych, żeby dwa skrypty nie zapisywały w nim jednocześnie.

    Windows zwalnia blokadę po zamknięciu pliku, także po przerwaniu programu.
    """
    with (root / ".download.lock").open("a+b") as lock:
        if lock.tell() == 0:
            lock.write(b"0")
            lock.flush()
        lock.seek(0)
        try:
            msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError:
            raise RuntimeError("Folder data/ jest już używany przez inny skrypt.")
        yield


def download_zip(url, path):
    """Pobiera ZIP w częściach po 1 MB. Po błędzie połączenia ponawia próbę najwyżej dwa razy."""
    temporary = path.with_suffix(".zip.part")
    for attempt in range(1, 4):
        try:
            with urllib.request.urlopen(url, timeout=90) as source, temporary.open("wb") as target:
                expected = int(source.headers.get("Content-Length", 0))
                size = 0
                while chunk := source.read(1024 * 1024):
                    size += len(chunk)
                    if size > 100_000_000:
                        raise ValueError("Plik ZIP jest większy niż 100 MB.")
                    target.write(chunk)
            if size == 0 or (expected and size != expected):
                raise ConnectionError("pobrano niepełny plik")
            temporary.replace(path)
            print(f"  Pobrano {size / 1_000_000:.1f} MB")
            return
        except (urllib.error.URLError, TimeoutError, ConnectionError) as error:
            # Błąd typu 404 jest trwały, więc ponawianie nic nie da.
            if isinstance(error, urllib.error.HTTPError) and error.code not in (429, 500, 502, 503, 504):
                raise
            if attempt == 3:
                raise RuntimeError(f"Nie udało się pobrać pliku po 3 próbach: {error}")
            print(f"  Błąd połączenia: {error}. Ponowna próba za {15 * attempt} s.")
            time.sleep(15 * attempt)


def extract_csv(archive, temporary, label, columns):
    """Rozpakowuje CSV i sprawdza nagłówek, liczbę pól oraz daty. Zwraca nagłówek i wpis do manifestu."""
    with zipfile.ZipFile(archive) as zipped:
        files = [f for f in zipped.infolist() if f.filename.endswith(".csv") and not f.filename.startswith("__MACOSX/")]
        if len(files) != 1:
            raise ValueError("Archiwum ZIP powinno zawierać dokładnie jeden plik CSV.")
        info = files[0]
        existing = sum(p.stat().st_size for p in temporary.parent.glob("*.csv"))
        if info.file_size > GB or existing + info.file_size > MAX_CSV_BYTES:
            raise ValueError("Plik CSV przekracza limit (1 GB na miesiąc, 6 GB łącznie).")
        if shutil.disk_usage(temporary.parent).free < 15 * GB + info.file_size:
            raise RuntimeError("Za mało wolnego miejsca na dysku (potrzebne 15 GB zapasu).")
        with zipped.open(info) as source, temporary.open("wb") as target:
            shutil.copyfileobj(source, target, 1024 * 1024)
    with temporary.open(encoding="utf-8-sig", newline="") as stream:
        reader = csv.reader(stream, strict=True)
        header = next(reader, [])
        required = {"FlightDate", "Reporting_Airline", "Origin", "Dest", "CRSDepTime", "ArrDelay", "Cancelled", "Diverted"}
        if not required.issubset(header) or len(header) != len(set(header)) or (columns and header != columns):
            raise ValueError("Plik CSV ma inny schemat kolumn niż oczekiwany.")
        position = header.index("FlightDate")
        records, previous_line = 0, reader.line_num
        for row in reader:
            if len(row) != len(header) or reader.line_num != previous_line + 1:
                raise ValueError(f"Błędny wiersz {reader.line_num} w pliku CSV.")
            if date.fromisoformat(row[position]).strftime("%Y-%m") != label:
                raise ValueError(f"Wiersz {reader.line_num}: data lotu spoza miesiąca {label}.")
            records, previous_line = records + 1, reader.line_num
    if not records or temporary.stat().st_size != info.file_size:
        raise ValueError("Plik CSV jest pusty lub niekompletny.")
    return header, {"file": label + ".csv", "records": records, "bytes": info.file_size,
                    "sha256": checksum(temporary), "zip_bytes": archive.stat().st_size}


def download_month(root, label, manifest):
    """Pobiera jeden miesiąc albo sprawdza plik pobrany wcześniej."""
    final = root / (label + ".csv")
    temporary = root / (label + ".csv.part")
    archive = root / (label + ".zip")
    year, number = label.split("-")
    url = CATALOG + f"On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{int(number)}.zip"
    saved = manifest["files"].get(label)
    if saved:
        candidate = final if final.exists() else temporary
        if candidate.stat().st_size != saved["bytes"] or checksum(candidate) != saved["sha256"]:
            raise ValueError(f"Plik {label}.csv nie zgadza się z manifestem.")
        if candidate == temporary:
            temporary.replace(final)
        print(f"{label}: plik już pobrany i sprawdzony.")
    else:
        if final.exists():
            raise ValueError(f"Plik {label}.csv istnieje bez wpisu w manifeście.")
        if shutil.disk_usage(root).free < 15 * GB + 100_000_000:
            raise RuntimeError("Za mało wolnego miejsca na dysku (potrzebne 15 GB zapasu).")
        print(f"{label}: pobieranie...")
        if not archive.exists():
            download_zip(url, archive)
        try:
            columns, entry = extract_csv(archive, temporary, label, manifest["columns"])
        except (zipfile.BadZipFile, zlib.error, EOFError):
            archive.unlink(missing_ok=True)
            raise ValueError("Uszkodzony plik ZIP został usunięty. Uruchom skrypt ponownie.")
        entry["url"] = url
        manifest["columns"], manifest["files"][label] = columns, entry
        # Najpierw zapisujemy manifest. Jeśli program zostanie przerwany przed zmianą nazwy,
        # plik .csv.part da się odzyskać bez ponownego pobierania.
        save_manifest(root, manifest)
        temporary.replace(final)
        print(f"{label}: gotowe, {entry['records']} lotów.")
    archive.unlink(missing_ok=True)
    archive.with_suffix(".zip.part").unlink(missing_ok=True)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Pokaz plan bez pobierania.")
    args = parser.parse_args(argv)
    print(f"Pobieranie {len(MONTHS)} miesięcy: {MONTHS[0]} – {MONTHS[-1]}.")
    if args.dry_run:
        return
    OUTPUT.mkdir(parents=True, exist_ok=True)
    with directory_lock(OUTPUT):
        path = OUTPUT / "manifest.json"
        manifest = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {
            "dataset": DATASET, "catalog_url": CATALOG, "format": "csv", "columns": [], "files": {}}
        if manifest.get("dataset") != DATASET or manifest.get("format") != "csv":
            raise ValueError("Plik data/manifest.json nie opisuje danych o lotach.")
        for label in MONTHS:
            download_month(OUTPUT, label, manifest)
        entries = [manifest["files"][label] for label in MONTHS]
        total = sum(e["bytes"] for e in entries)
        print(f"GOTOWE: {len(entries)} plików, {total / GB:.2f} GB w folderze data/.")
        if total < 5.5 * GB:
            print("Uwaga: dane zajmują mniej niż 5,5 GB.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit("\nPrzerwano. Uruchom skrypt ponownie, aby wznowić pobieranie.")
    except (OSError, ValueError, RuntimeError, csv.Error) as error:
        sys.exit(f"BŁĄD: {error}\nGotowe miesiące zostały zachowane. Uruchom skrypt ponownie.")
