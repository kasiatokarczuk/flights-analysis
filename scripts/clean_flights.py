"""Mapper Hadoop Streaming: sprawdza i czyści rekordy lotów.

Wejście: surowe CSV z /flights/raw/. Hadoop dzieli duże pliki na fragmenty, więc
tylko pierwszy fragment pliku ma nagłówek - kolejność kolumn bierzemy z manifestu.
Wyjście: CSV z 21 wybranymi kolumnami, bez nagłówka.
Liczniki (ile rekordów, ile odrzuconych, ile braków) trafiają do Hadoop przez stderr.
"""
import csv
import json
import math
import os
import sys
from collections import Counter
from datetime import date
from pathlib import Path

COLUMNS = [
    "FlightDate", "Reporting_Airline", "Flight_Number_Reporting_Airline",
    "OriginAirportID", "Origin", "OriginCityName",
    "DestAirportID", "Dest", "DestCityName", "CRSDepTime",
    "DepDelay", "ArrDelay", "Cancelled", "Diverted", "Distance", "AirTime",
    "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay",
]
NUMBERS = ["DepDelay", "ArrDelay", "Distance", "AirTime",
           "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay"]
CAN_BE_NEGATIVE = {"DepDelay", "ArrDelay"}  # lot przed czasem ma ujemne opóźnienie


def as_flag(value):
    """'0', '0.00', '1', '1.00' -> '0' lub '1'. Inne wartości -> None."""
    try:
        return {0.0: "0", 1.0: "1"}.get(float(value))
    except ValueError:
        return None


def is_valid_time(value):
    """Godzina HHMM; źródło dopuszcza też 2400 (północ)."""
    if not (value.isascii() and value.isdigit() and len(value) <= 4):
        return False
    number = int(value)
    return number == 2400 or (number < 2400 and number % 100 < 60)


def reject_reason(flight, month):
    """Zwraca powód odrzucenia rekordu albo None, gdy rekord jest poprawny."""
    try:
        day = date.fromisoformat(flight["FlightDate"])
        if day.isoformat() != flight["FlightDate"] or day.strftime("%Y-%m") != month:
            return "date"
    except ValueError:
        return "date"
    if not flight["Reporting_Airline"]:
        return "carrier"
    for name in ("Origin", "Dest"):
        if not (len(flight[name]) == 3 and flight[name].isascii() and flight[name].isalpha()):
            return "airport"
    for name in ("OriginAirportID", "DestAirportID"):
        if not (flight[name].isascii() and flight[name].isdigit()):
            return "airport"
    if None in (flight["Cancelled"], flight["Diverted"]):
        return "status"
    if flight["Cancelled"] == flight["Diverted"] == "1":
        return "status"  # lot nie może być jednocześnie odwołany i przekierowany
    return None


def clean_row(row, month, counts):
    """Zwraca oczyszczony rekord (słownik) albo None, gdy rekord odrzucono."""
    flight = {name: row[name].strip() for name in COLUMNS}
    flight["Origin"] = flight["Origin"].upper()
    flight["Dest"] = flight["Dest"].upper()
    flight["Cancelled"] = as_flag(flight["Cancelled"])
    flight["Diverted"] = as_flag(flight["Diverted"])

    reason = reject_reason(flight, month)
    if reason:
        counts["rejected_" + reason] += 1
        return None

    # Pola opcjonalne: brak zostaje brakiem, błędna wartość staje się brakiem (nie zerem).
    time = flight["CRSDepTime"]
    if not time:
        counts["missing_CRSDepTime"] += 1
    elif is_valid_time(time):
        flight["CRSDepTime"] = time.zfill(4)
    else:
        flight["CRSDepTime"] = ""
        counts["invalid_CRSDepTime"] += 1

    for name in NUMBERS:
        if not flight[name]:
            counts["missing_" + name] += 1
            continue
        try:
            value = float(flight[name])
            valid = math.isfinite(value) and (value >= 0 or name in CAN_BE_NEGATIVE)
        except ValueError:
            valid = False
        if not valid:
            flight[name] = ""
            counts["invalid_" + name] += 1

    if flight["Cancelled"] == "1":
        counts["cancelled"] += 1
    elif flight["Diverted"] == "1":
        counts["diverted"] += 1
    else:
        counts["regular"] += 1
    return flight


def mapper():
    header = json.loads(Path("manifest.json").read_text(encoding="utf-8"))["columns"]
    position = {name: index for index, name in enumerate(header)}
    # Hadoop podaje nazwę przetwarzanego pliku, np. .../2024-01.csv -> miesiąc 2024-01.
    month = Path(os.environ["mapreduce_map_input_file"]).stem

    counts = Counter()
    writer = csv.writer(sys.stdout, lineterminator="\n")
    for line in sys.stdin:
        try:
            values = next(csv.reader([line.lstrip("﻿")], strict=True))
        except csv.Error:
            values = []
        if values == header:
            counts["headers"] += 1
            continue
        counts["records"] += 1
        counts["month_" + month] += 1
        if len(values) != len(header):
            counts["rejected_structure"] += 1
            continue
        flight = clean_row({name: values[position[name]] for name in COLUMNS}, month, counts)
        if flight:
            writer.writerow(flight[name] for name in COLUMNS)
            counts["written"] += 1

    for name, value in sorted(counts.items()):
        print(f"reporter:counter:FlightQuality,{name},{value}", file=sys.stderr)


if __name__ == "__main__":
    mapper()
