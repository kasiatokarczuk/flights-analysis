"""Import do MongoDB: budowa dokumentów, ponowny import bez duplikatów i usuwanie nieaktualnych danych.

Zamiast prawdziwego serwera używamy mongomock - bazy MongoDB działającej w pamięci.
"""
import json
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import mongomock
from mongomock.collection import BulkOperationBuilder

# Nowsze pymongo przekazuje przy zapisie partiami parametr sort, którego mongomock jeszcze nie zna.
# Prawdziwy serwer go obsługuje; w atrapie po prostu go pomijamy.
_add_replace = BulkOperationBuilder.add_replace
BulkOperationBuilder.add_replace = lambda self, *args, sort=None, **kwargs: _add_replace(self, *args, **kwargs)

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import load_mongo as loader

STATS = {"flights": 2, "cancelled_pct": 0.0, "arr_delayed_pct": 50.0}
SAMPLE = [
    '2025-09-01,9E,4800,10994,CHS,"Charleston, SC",12478,JFK,"New York, NY",0700,-4.00,0.00,0,0,636.00,91.00,,,,,\n',
    "2025-09-02,AA,1,12892,LAX,Los Angeles,12478,JFK,New York,0800,,,1,0,2475.00,,,,,,\n",
]


class LoadMongoTests(unittest.TestCase):
    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.root = Path(folder.name)
        (self.root / "results").mkdir()
        (self.root / "data" / "scraped").mkdir(parents=True)
        self.analysis = {"application_id": "application_1", "finished_utc": "2026-11-05T22:16:00+00:00",
                         "hdfs_input": "/flights/clean", "hdfs_output": "/flights/analysis",
                         "results": {"total": {"all": STATS}, "carrier": {"9E": STATS, "AA": STATS}}}
        self.write("results/analysis.json", self.analysis)
        self.write("results/airports.json", [{"iata": "DJT", "bts_code": "PBI", "role_group": "średnie"},
                                             {"iata": "JFK", "bts_code": "JFK", "role_group": "duże"}])
        self.write("results/carriers.json", [{"iata": "9E", "name": "Endeavor Air"}])
        self.write("results/data_quality.json", {"counters": {"written": 2}})
        self.write("data/manifest.json", {"dataset": "flights", "files": {"2025-09": {"records": 2}}})
        self.write("data/scraped/airports.json", {"url": "https://example.test", "fetched_utc": "2026-11-05"})
        self.database = mongomock.MongoClient()["flights"]
        for patcher in (patch.object(loader, "ROOT", self.root), patch.object(loader, "RESULTS", self.root / "results"),
                        patch.object(loader, "connect", return_value=self.database), patch("builtins.print")):
            patcher.start()
            self.addCleanup(patcher.stop)

    def write(self, name, data):
        (self.root / name).write_text(json.dumps(data), encoding="utf-8")

    def test_documents_have_stable_ids(self):
        docs = loader.documents()
        self.assertEqual(sorted(d["_id"] for d in docs["analysis"]), ["carrier|9E", "carrier|AA", "total|all"])
        self.assertEqual([d["_id"] for d in docs["airports"]], ["PBI", "JFK"])  # kod z danych BTS
        self.assertEqual({d["_id"] for d in docs["metadata"]}, {"manifest", "cleaning", "analysis", "scraping"})

    def test_flight_document_types(self):
        doc = loader.flight_document(next(loader.csv.reader(SAMPLE[:1])))
        self.assertEqual(doc["_id"], "2025-09-01|9E|4800|CHS|JFK|0700")
        self.assertEqual(doc["FlightDate"], datetime(2025, 9, 1))
        self.assertEqual((doc["ArrDelay"], doc["CarrierDelay"], doc["Cancelled"]), (0.0, None, False))
        self.assertEqual(doc["OriginCityName"], "Charleston, SC")

    def test_second_import_does_not_duplicate(self):
        loader.load("local", sample=True, lines=SAMPLE)
        loader.load("local", sample=True, lines=SAMPLE)
        self.assertEqual(self.database["analysis"].count_documents({}), 3)
        self.assertEqual(self.database["flights_sample"].count_documents({}), 2)
        second = self.database["import_log"].find_one({}, sort=[("_id", -1)])["collections"]
        self.assertEqual(second["analysis"], {"nowe": 0, "zmienione": 0, "bez zmian": 3, "usunięte": 0})
        self.assertEqual(second["flights_sample"]["nowe"], 0)
        self.assertEqual(self.database["import_log"].count_documents({}), 2)  # jeden wpis na uruchomienie

    def test_changed_and_removed_results_are_synchronised(self):
        loader.load("local", sample=False)
        del self.analysis["results"]["carrier"]["AA"]
        self.analysis["results"]["carrier"]["9E"] = dict(STATS, flights=5)
        self.write("results/analysis.json", self.analysis)
        loader.load("local", sample=False)
        last = self.database["import_log"].find_one({}, sort=[("_id", -1)])["collections"]["analysis"]
        self.assertEqual((last["zmienione"], last["usunięte"]), (1, 1))
        self.assertEqual(self.database["analysis"].find_one({"_id": "carrier|9E"})["flights"], 5)
        self.assertIsNone(self.database["analysis"].find_one({"_id": "carrier|AA"}))

    def test_sample_from_another_month_is_replaced(self):
        self.database["flights_sample"].insert_one({"_id": "2025-08-31|AA|1|LAX|JFK|0800"})
        total, counts = loader.sync_sample(self.database["flights_sample"], SAMPLE, "2025-09")
        self.assertEqual((total, counts["nowe"], counts["usunięte"]), (2, 2, 1))
        self.assertEqual(self.database["flights_sample"].count_documents({}), 2)


if __name__ == "__main__":
    unittest.main()
