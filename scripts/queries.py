"""Zapytania do MongoDB używane przez panel (dashboard.py) i wykresy (charts.py).

Każda funkcja dostaje bazę projektu (db.connect) i zwraca tabelę pandas.
"""
import pandas as pd

SIZE_ORDER = ["duże", "średnie", "małe", "bardzo małe", "inne / brak", "brak w tabeli"]


def total(database):
    """Wskaźniki dla całego zbioru (dokument total|all)."""
    return database["analysis"].find_one({"_id": "total|all"})


def group_table(database, group):
    """Wyniki jednej analizy, np. group="month". Dodaje odsetek lotów na czas."""
    rows = list(database["analysis"].find({"group": group}, {"_id": 0}))
    table = pd.DataFrame(rows).sort_values("key").reset_index(drop=True)
    table["on_time_pct"] = 100 - table["arr_delayed_pct"]
    return table


def carriers(database):
    """Wyniki przewoźników złączone z danymi ze scrapingu (nazwa, typ, marka)."""
    pipeline = [
        {"$match": {"group": "carrier"}},
        {"$lookup": {"from": "carriers", "localField": "key", "foreignField": "_id", "as": "info"}},
        {"$unwind": {"path": "$info", "preserveNullAndEmptyArrays": True}},
        {"$project": {"_id": 0, "key": 1, "flights": 1, "arr_delayed_pct": 1, "arr_delay_mean": 1,
                      "arr_delay_median": 1, "cancelled_pct": 1, "cause_share_pct": 1,
                      "name": {"$ifNull": ["$info.name", "$key"]},
                      "type": {"$ifNull": ["$info.type", "nieznany"]},
                      "operates_as": "$info.operates_as"}},
    ]
    table = pd.DataFrame(database["analysis"].aggregate(pipeline))
    table["on_time_pct"] = 100 - table["arr_delayed_pct"]
    table["label"] = table["name"] + " (" + table["key"] + ")"
    return table.sort_values("on_time_pct", ascending=False).reset_index(drop=True)


def airports_grouped(database, field):
    """Wyniki lotnisk złączone ze scrapingiem i zsumowane według pola z kolekcji airports
    (role_group - wielkość lotniska, state - stan). Odsetki są ważone liczbą lotów."""
    pipeline = [
        {"$match": {"group": "airport"}},
        {"$lookup": {"from": "airports", "localField": "key", "foreignField": "_id", "as": "info"}},
        {"$unwind": {"path": "$info", "preserveNullAndEmptyArrays": True}},
        {"$group": {"_id": {"$ifNull": ["$info." + field, "brak w tabeli"]},
                    "airports": {"$sum": 1},
                    "flights": {"$sum": "$flights"},
                    "delayed": {"$sum": {"$multiply": ["$flights", "$dep_delayed_pct"]}},
                    "cancelled": {"$sum": {"$multiply": ["$flights", "$cancelled_pct"]}}}},
        {"$project": {"_id": 0, "name": "$_id", "airports": 1, "flights": 1,
                      "dep_delayed_pct": {"$divide": ["$delayed", "$flights"]},
                      "cancelled_pct": {"$divide": ["$cancelled", "$flights"]}}},
    ]
    table = pd.DataFrame(database["analysis"].aggregate(pipeline))
    if field == "role_group":
        table["order"] = table["name"].map(SIZE_ORDER.index)
        return table.sort_values("order").drop(columns="order").reset_index(drop=True)
    return table.sort_values("flights", ascending=False).reset_index(drop=True)


def sample_flights(database, carrier=None, origin=None, cancelled_only=False, limit=200):
    """Loty z próbki spełniające filtry: podsumowanie liczone w MongoDB i lista najbardziej opóźnionych."""
    query = {}
    if carrier:
        query["Reporting_Airline"] = carrier
    if origin:
        query["Origin"] = origin
    if cancelled_only:
        query["Cancelled"] = True
    summary = list(database["flights_sample"].aggregate([
        {"$match": query},
        {"$group": {"_id": None, "flights": {"$sum": 1},
                    "cancelled": {"$sum": {"$cond": ["$Cancelled", 1, 0]}},
                    "arr_delay_mean": {"$avg": "$ArrDelay"}}},
    ]))
    rows = database["flights_sample"].find(query, {"_id": 0}).sort("ArrDelay", -1).limit(limit)
    return (summary[0] if summary else None), pd.DataFrame(rows)


def last_import(database):
    """Ostatni wpis z import_log."""
    return database["import_log"].find_one(sort=[("started_utc", -1)])
