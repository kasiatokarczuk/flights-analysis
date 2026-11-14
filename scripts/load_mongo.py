"""Import wyników do baz MongoDB: lokalnej i Atlas.

    python scripts/load_mongo.py                 # obie bazy
    python scripts/load_mongo.py --target local  # tylko lokalna (local, atlas albo all)
    python scripts/load_mongo.py --no-sample     # bez próbki lotów

Kolekcje w bazie flights:
    analysis        wyniki analiz MapReduce, jeden dokument na grupę (_id np. "carrier|WN"),
    airports        lotniska ze scrapingu Wikipedii (_id = kod lotniska w danych BTS),
    carriers        przewoźnicy ze scrapingu (_id = kod IATA),
    metadata        manifest danych, raport jakości, informacje o przebiegach,
    import_log      jeden wpis na każde uruchomienie importu,
    flights_sample  oczyszczone loty z jednego miesiąca, czytane z HDFS (tylko baza lokalna).

Każdy dokument ma stały _id i jest zapisywany operacją "zastąp albo wstaw" (upsert),
więc ponowny import aktualizuje dokumenty zamiast je powielać. Dokumenty, których nie ma
już w plikach źródłowych, są usuwane - kolekcja zawsze odpowiada aktualnym wynikom.
"""
import argparse
import csv
import json
import subprocess
import sys
from datetime import datetime, timezone

from pymongo import ASCENDING, ReplaceOne
from pymongo.errors import PyMongoError

from clean_flights import COLUMNS, FLIGHT_KEY, NUMBERS
from common import ROOT, RESULTS, number
from db import TARGETS, connect

SAMPLE_MONTH = "2025-09"
BATCH = 5000
INDEXES = {
    "analysis": ["group"],
    "airports": ["role_group", "state"],
    "carriers": ["type"],
    "flights_sample": ["FlightDate", "Reporting_Airline", "Origin", "Dest"],
}


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def documents():
    """Buduje dokumenty wszystkich małych kolekcji z plików w results/ i data/."""
    analysis = read_json(RESULTS / "analysis.json")
    run = {"application_id": analysis["application_id"], "finished_utc": analysis["finished_utc"]}
    scraped = read_json(ROOT / "data" / "scraped" / "airports.json")
    return {
        "analysis": [{"_id": f"{group}|{key}", "group": group, "key": key, **stats}
                     for group, rows in analysis["results"].items() for key, stats in rows.items()],
        "airports": [{"_id": airport["bts_code"], **airport}
                     for airport in read_json(RESULTS / "airports.json") if airport["bts_code"]],
        "carriers": [{"_id": carrier["iata"], **carrier} for carrier in read_json(RESULTS / "carriers.json")],
        "metadata": [
            {"_id": "manifest", **read_json(ROOT / "data" / "manifest.json")},
            {"_id": "cleaning", **read_json(RESULTS / "data_quality.json")},
            {"_id": "analysis", **run, "hdfs_input": analysis["hdfs_input"], "hdfs_output": analysis["hdfs_output"]},
            {"_id": "scraping", "airports_url": scraped["url"], "fetched_utc": scraped["fetched_utc"]},
        ],
    }


def write(collection, docs, counts):
    """Zapisuje partię dokumentów przez upsert po _id i dolicza wynik do counts."""
    result = collection.bulk_write([ReplaceOne({"_id": d["_id"]}, d, upsert=True) for d in docs], ordered=False)
    counts["nowe"] += result.upserted_count
    counts["zmienione"] += result.modified_count
    counts["bez zmian"] += result.matched_count - result.modified_count


def new_counts():
    return {"nowe": 0, "zmienione": 0, "bez zmian": 0, "usunięte": 0}


def sync(collection, docs):
    """Ustawia zawartość kolekcji na podane dokumenty: upsert każdego i usunięcie pozostałych."""
    counts = new_counts()
    if docs:
        write(collection, docs, counts)
    counts["usunięte"] = collection.delete_many({"_id": {"$nin": [d["_id"] for d in docs]}}).deleted_count
    return counts


def flight_document(row):
    """Zamienia wiersz oczyszczonego CSV na dokument: liczby jako liczby, status jako True/False."""
    flight = dict(zip(COLUMNS, row))
    doc = {"_id": "|".join(flight[name] for name in FLIGHT_KEY)}  # ten sam klucz co przy usuwaniu duplikatów
    for name, value in flight.items():
        if name in NUMBERS:
            doc[name] = float(value) if value else None
        elif name in ("Cancelled", "Diverted"):
            doc[name] = value == "1"
        elif name == "FlightDate":
            doc[name] = datetime.fromisoformat(value)
        else:
            doc[name] = value or None
    return doc


def sync_sample(collection, lines, month):
    """Zapisuje loty jednego miesiąca partiami (bez trzymania wszystkich w pamięci).
    Loty z innych miesięcy (np. z wcześniejszej próbki) są usuwane. Zwraca (liczba lotów, counts)."""
    counts, batch, total = new_counts(), [], 0
    for row in csv.reader(lines):
        batch.append(flight_document(row))
        if len(batch) == BATCH:
            write(collection, batch, counts)
            total, batch = total + len(batch), []
    if batch:
        write(collection, batch, counts)
        total += len(batch)
    counts["usunięte"] = collection.delete_many({"_id": {"$not": {"$regex": f"^{month}-"}}}).deleted_count
    return total, counts


def hdfs_month(month):
    """Strumień linii oczyszczonego CSV z jednego miesiąca. Filtr działa w kontenerze,
    więc do Windows trafia tylko ten miesiąc, a nie cały 1,4 GB wynik czyszczenia."""
    command = ["docker", "compose", "exec", "-T", "resourcemanager", "bash", "-o", "pipefail", "-c",
               f"hdfs dfs -cat /flights/clean/part-* | grep '^{month}-'"]
    process = subprocess.Popen(command, cwd=ROOT, stdout=subprocess.PIPE, text=True, encoding="utf-8")
    yield from process.stdout
    if process.wait():
        raise RuntimeError("Nie udało się odczytać próbki lotów z HDFS. Czy kontenery Hadoop działają?")


def report(target, name, total, counts):
    print(f"{target}.{name}: liczba dokumentów {number(total)} ("
          + ", ".join(f"{label}: {number(value)}" for label, value in counts.items()) + ")")


def check_count(database, name, expected, target):
    stored = database[name].count_documents({})
    if stored != expected:
        raise ValueError(f"{target}.{name}: w bazie {stored} dokumentów, w źródle {expected}.")


def load(target, sample, lines=None):
    """Importuje wszystkie kolekcje do jednej bazy i sprawdza liczbę dokumentów.
    lines: linie próbki lotów (domyślnie czytane z HDFS)."""
    database = connect(target)
    started = datetime.now(timezone.utc)
    summary = {}
    for name, docs in documents().items():
        summary[name] = sync(database[name], docs)
        check_count(database, name, len(docs), target)
        report(target, name, len(docs), summary[name])
    if sample:
        print(f"{target}: wczytywanie próbki lotów {SAMPLE_MONTH} z HDFS...")
        total, summary["flights_sample"] = sync_sample(
            database["flights_sample"], lines if lines is not None else hdfs_month(SAMPLE_MONTH), SAMPLE_MONTH)
        check_count(database, "flights_sample", total, target)
        report(target, "flights_sample", total, summary["flights_sample"])
    for name, fields in INDEXES.items():
        if name == "flights_sample" and not sample:
            continue
        for field in fields:
            database[name].create_index([(field, ASCENDING)])
    database["import_log"].insert_one({"started_utc": started, "finished_utc": datetime.now(timezone.utc),
                                       "target": target, "collections": summary})


def main():
    parser = argparse.ArgumentParser(description="Import wyników do MongoDB.")
    parser.add_argument("--target", choices=TARGETS + ["all"], default="all")
    parser.add_argument("--no-sample", action="store_true", help="nie importuj próbki lotów")
    args = parser.parse_args()
    for target in TARGETS if args.target == "all" else [args.target]:
        # Próbka lotów trafia tylko do bazy lokalnej - darmowy Atlas ma limit 512 MB.
        load(target, sample=target == "local" and not args.no_sample)
    print("GOTOWE.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit("\nPrzerwano. Ponowne uruchomienie dokończy import bez powielania danych.")
    except (OSError, ValueError, KeyError, RuntimeError, PyMongoError) as error:
        sys.exit(f"BŁĄD: {error}")
