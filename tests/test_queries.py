"""Zapytania panelu: złączenie ($lookup) wyników analiz z danymi ze scrapingu."""
import sys
import unittest
from pathlib import Path

import mongomock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import queries


def stats(flights, delayed, cancelled=0.0):
    return {"flights": flights, "dep_delayed_pct": delayed, "arr_delayed_pct": delayed,
            "cancelled_pct": cancelled, "arr_delay_mean": 1.0, "arr_delay_median": 0.0,
            "cause_share_pct": {}}


class QueryTests(unittest.TestCase):
    def setUp(self):
        self.db = mongomock.MongoClient()["flights"]
        self.db["analysis"].insert_many([
            {"_id": "airport|ATL", "group": "airport", "key": "ATL", **stats(300, 30.0)},
            {"_id": "airport|JFK", "group": "airport", "key": "JFK", **stats(100, 10.0)},
            {"_id": "airport|DHN", "group": "airport", "key": "DHN", **stats(50, 20.0, 2.0)},
            {"_id": "airport|XXX", "group": "airport", "key": "XXX", **stats(10, 50.0)},
            {"_id": "carrier|9E", "group": "carrier", "key": "9E", **stats(10, 20.0)},
            {"_id": "carrier|ZZ", "group": "carrier", "key": "ZZ", **stats(10, 40.0)},
        ])
        self.db["airports"].insert_many([
            {"_id": "ATL", "role_group": "duże", "state": "Georgia"},
            {"_id": "JFK", "role_group": "duże", "state": "New York"},
            {"_id": "DHN", "role_group": "bardzo małe", "state": "Alabama"},
        ])
        self.db["carriers"].insert_one({"_id": "9E", "name": "Endeavor Air", "type": "regional"})

    def test_airports_by_size_are_weighted_by_flights(self):
        table = queries.airports_grouped(self.db, "role_group").set_index("name")
        self.assertEqual(list(table.index), ["duże", "bardzo małe", "brak w tabeli"])
        self.assertEqual(table.loc["duże", "airports"], 2)
        self.assertAlmostEqual(table.loc["duże", "dep_delayed_pct"], 25.0)  # (300*30 + 100*10) / 400
        self.assertAlmostEqual(table.loc["bardzo małe", "cancelled_pct"], 2.0)

    def test_airports_by_state_sorted_by_flights(self):
        self.assertEqual(list(queries.airports_grouped(self.db, "state")["name"]),
                         ["Georgia", "New York", "Alabama", "brak w tabeli"])

    def test_carriers_get_names_from_scraping(self):
        table = queries.carriers(self.db).set_index("key")
        self.assertEqual(table.loc["9E", "label"], "Endeavor Air (9E)")
        self.assertEqual((table.loc["ZZ", "name"], table.loc["ZZ", "type"]), ("ZZ", "nieznany"))
        self.assertEqual(list(table.index), ["9E", "ZZ"])  # od najbardziej punktualnego


if __name__ == "__main__":
    unittest.main()
