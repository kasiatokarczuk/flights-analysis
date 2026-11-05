"""Mapper i reducer Hadoop Streaming: sprawdzają i czyszczą rekordy lotów.

Mapper (python3 clean_flights.py map):
    czyta surowe CSV z /flights/raw/, sprawdza każdy rekord i wypisuje go
    w postaci "klucz lotu<TAB>rekord CSV". Hadoop dzieli duże pliki na fragmenty,
    więc tylko pierwszy fragment pliku ma nagłówek - kolejność kolumn bierzemy z manifestu.
Reducer (python3 clean_flights.py reduce):
    dostaje rekordy posortowane po kluczu lotu, więc duplikaty leżą obok siebie.
    Zapisuje pierwszy rekord z każdej grupy, a pozostałe liczy jako duplikaty.

Wynik: CSV z 21 wybranymi kolumnami, bez nagłówka.
Liczniki (ile rekordów, ile odrzuconych, ile braków) trafiają do Hadoop przez stderr.
"""
import csv
import io
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
# Jeden lot = ta sama data, przewoźnik, numer lotu, trasa i planowana godzina odlotu.
FLIGHT_KEY = ["FlightDate", "Reporting_Airline", "Flight_Number_Reporting_Airline",
              "Origin", "Dest", "CRSDepTime"]
MIN_SPEED, MAX_SPEED = 100, 700  # realna średnia prędkość lotu w milach na godzinę
EXTREME_DELAY = 24 * 60  # opóźnienie przylotu powyżej doby (w minutach)


def as_flag(value):
    """Zamienia status zapisany jako 0/1 lub 0.00/1.00 na '0' albo '1'. Dla innych wartości zwraca None."""
    try:
        return {0.0: "0", 1.0: "1"}.get(float(value))
    except ValueError:
        return None


def is_valid_time(value):
    """Sprawdza, czy godzina ma format HHMM. Źródło dopuszcza też 2400 (północ)."""
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
    if flight["Cancelled"] == flight["Diverted"] == "0" and not flight["ArrDelay"]:
        return "no_arrival"  # lot się odbył, więc powinien mieć opóźnienie przylotu
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

    # Czas w powietrzu jest nierealny, jeśli wychodzi z niego prędkość poza zakresem.
    if flight["AirTime"] and flight["Distance"]:
        hours = float(flight["AirTime"]) / 60
        if hours == 0 or not MIN_SPEED <= float(flight["Distance"]) / hours <= MAX_SPEED:
            flight["AirTime"] = ""
            counts["implausible_AirTime"] += 1

    # Bardzo duże opóźnienia zostawiamy (mogą być prawdziwe), ale je liczymy.
    if flight["ArrDelay"] and float(flight["ArrDelay"]) > EXTREME_DELAY:
        counts["extreme_ArrDelay"] += 1

    if flight["Cancelled"] == "1":
        counts["cancelled"] += 1
    elif flight["Diverted"] == "1":
        counts["diverted"] += 1
    else:
        counts["regular"] += 1
    return flight


def to_csv(values):
    """Zamienia listę wartości na jedną linię CSV (pola z przecinkiem trafiają w cudzysłów)."""
    buffer = io.StringIO()
    csv.writer(buffer, lineterminator="").writerow(values)
    return buffer.getvalue()


def report_counters(counts):
    """Przekazuje liczniki do Hadoop w formacie reporter:counter:grupa,nazwa,wartość."""
    for name, value in sorted(counts.items()):
        print(f"reporter:counter:FlightQuality,{name},{value}", file=sys.stderr)


def mapper():
    header = json.loads(Path("manifest.json").read_text(encoding="utf-8"))["columns"]
    position = {name: index for index, name in enumerate(header)}
    # Hadoop podaje nazwę przetwarzanego pliku, np. .../2024-01.csv -> miesiąc 2024-01.
    month = Path(os.environ["mapreduce_map_input_file"]).stem

    counts = Counter()
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
            key = "|".join(flight[name] for name in FLIGHT_KEY)
            print(key + "\t" + to_csv(flight[name] for name in COLUMNS))
            counts["valid"] += 1
    report_counters(counts)


def reducer():
    counts = Counter()
    previous_key = None
    for line in sys.stdin:
        key, row = line.rstrip("\n").split("\t", 1)
        if key == previous_key:
            counts["duplicates"] += 1
            continue
        previous_key = key
        print(row)
        counts["written"] += 1
    report_counters(counts)


if __name__ == "__main__":
    if sys.argv[1] == "map":
        mapper()
    else:
        reducer()
