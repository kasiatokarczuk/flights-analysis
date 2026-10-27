"""Mapper Hadoop Streaming: sprawdzanie i czyszczenie danych o lotach."""

import csv
import json
import math
import os
import sys
from collections import Counter
from datetime import date
from pathlib import Path


# Kolejność pól w oczyszczonym CSV; oryginalne pliki pozostają bez zmian.
COLUMNS = [
    "FlightDate", "Reporting_Airline", "Flight_Number_Reporting_Airline",
    "OriginAirportID", "Origin", "OriginCityName",
    "DestAirportID", "Dest", "DestCityName", "CRSDepTime",
    "DepDelay", "ArrDelay", "Cancelled", "Diverted", "Distance", "AirTime",
    "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay",
]
NUMBERS = ["DepDelay", "ArrDelay", "Distance", "AirTime",
           "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay"]


def clean_row(row, month, counts):
    """Zwróć oczyszczony rekord lub None. Licz pierwszy powód odrzucenia."""
    flight = {name: row[name].strip() for name in COLUMNS}
    if any(flight[name] != row[name] for name in COLUMNS):
        counts["trimmed_rows"] += 1

    # Data musi być poprawna i należeć do miesiąca pliku źródłowego.
    try:
        day = date.fromisoformat(flight["FlightDate"])
        if day.isoformat() != flight["FlightDate"] or day.strftime("%Y-%m") != month:
            raise ValueError
    except ValueError:
        counts["rejected_date"] += 1
        return None

    if not flight["Reporting_Airline"]:
        counts["rejected_carrier"] += 1
        return None
    for name in ("Origin", "Dest"):
        code = flight[name].upper()
        if len(code) != 3 or not code.isascii() or not code.isalpha():
            counts["rejected_airport"] += 1
            return None
        if code != flight[name]:
            counts["airport_case_fixed"] += 1
        flight[name] = code
    for name in ("OriginAirportID", "DestAirportID"):
        if not flight[name].isascii() or not flight[name].isdigit() or int(flight[name]) <= 0:
            counts["rejected_airport_id"] += 1
            return None

    # Ujednolicamy zapis 0.00/1.00; sprzeczne statusy wykluczają rekord.
    try:
        flags = [float(flight[name]) for name in ("Cancelled", "Diverted")]
        if any(value not in (0, 1) for value in flags) or flags == [1, 1]:
            raise ValueError
    except ValueError:
        counts["rejected_status"] += 1
        return None
    normalized = [str(int(value)) for value in flags]
    if normalized != [flight["Cancelled"], flight["Diverted"]]:
        counts["status_normalized_rows"] += 1
    flight["Cancelled"], flight["Diverted"] = normalized

    # Godzina ma format HHMM; źródło dopuszcza również 2400.
    time = flight["CRSDepTime"]
    if not time:
        counts["missing_CRSDepTime"] += 1
    elif (time.isascii() and time.isdigit() and len(time) <= 4
          and (int(time) == 2400 or (int(time) < 2400 and int(time) % 100 < 60))):
        flight["CRSDepTime"] = time.zfill(4)
        counts["time_normalized_rows"] += flight["CRSDepTime"] != time
    else:
        flight["CRSDepTime"] = ""
        counts["invalid_CRSDepTime"] += 1

    # Osobno liczymy braki i błędne liczby. Ujemne opóźnienia są poprawne.
    for name in NUMBERS:
        if not flight[name]:
            counts["missing_" + name] += 1
            continue
        try:
            value = float(flight[name])
            if not math.isfinite(value) or (name not in ("DepDelay", "ArrDelay") and value < 0):
                raise ValueError
        except ValueError:
            flight[name] = ""
            counts["invalid_" + name] += 1

    status = "cancelled" if flags[0] else "diverted" if flags[1] else "regular"
    counts[status] += 1
    if not flight["ArrDelay"]:
        counts["missing_arrival_" + status] += 1
    return flight


def mapper():
    # Schemat jest dostępny także wtedy, gdy fragment danych nie ma nagłówka.
    manifest = json.loads(Path("manifest.json").read_text(encoding="utf-8"))
    header = manifest["columns"]
    if not set(COLUMNS).issubset(header) or len(set(header)) != len(header):
        raise ValueError("Niepoprawny schemat w manifescie.")
    positions = {name: index for index, name in enumerate(header)}
    selected = [(name, positions[name]) for name in COLUMNS]
    empty_at = positions.get("")
    source = os.environ.get("mapreduce_map_input_file") or os.environ.get("map_input_file", "")
    month = Path(source).stem
    if month not in manifest["files"]:
        raise ValueError("Brak rozpoznanej nazwy pliku wejsciowego Hadoop.")

    counts = Counter()
    writer = csv.writer(sys.stdout, lineterminator="\n")
    for line in sys.stdin:
        try:
            values = next(csv.reader([line.lstrip("\ufeff")], strict=True))
        except csv.Error:
            values = []
        # Nagłówek rozpoznajemy po treści, a nie po numerze wiersza.
        if values == header:
            counts["headers"] += 1
            continue
        counts["records"] += 1
        counts["month_" + month.replace("-", "_")] += 1
        if len(values) != len(header) or (empty_at is not None and values[empty_at] != ""):
            counts["rejected_structure"] += 1
            continue
        flight = clean_row({name: values[index] for name, index in selected}, month, counts)
        if flight is not None:
            # Runner ustawia przecinek między kluczem (datą) a resztą CSV.
            writer.writerow([flight[name] for name in COLUMNS])
            counts["written"] += 1

    # Dane trafiają do stdout, a liczniki do stderr w formacie Hadoop.
    for name, value in sorted(counts.items()):
        if value:
            print(f"reporter:counter:FlightQuality,{name},{value}", file=sys.stderr)


if __name__ == "__main__":
    mapper()
