"""Mapper i reducer Hadoop Streaming: statystyki opóźnień i odwołań lotów.

Mapper (python3 analyze_flights.py map):
    czyta oczyszczone CSV z /flights/clean/ i dla każdego lotu aktualizuje statystyki
    grup, do których lot należy: przewoźnik, miesiąc, dzień tygodnia, godzina odlotu,
    lotnisko wylotu oraz cały zbiór. Sumy trzyma w pamięci i wypisuje dopiero na końcu,
    więc do reducera trafia kilkaset linii zamiast milionów (in-mapper combining).
Reducer (python3 analyze_flights.py reduce):
    sumuje częściowe statystyki z wszystkich mapperów i liczy z nich wskaźniki.

Wynik: jedna linia na grupę w postaci "klucz<TAB>JSON", np. "carrier|WN<TAB>{...}".
"""
import csv
import json
import sys
from collections import Counter, defaultdict
from datetime import date
from itertools import groupby

from clean_flights import COLUMNS

CAUSES = ["CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay"]
DELAYED = 15  # tak jak BTS: lot jest opóźniony, jeśli spóźnił się o co najmniej 15 minut
SUMS = ["flights", "cancelled", "diverted", "arr_count", "arr_delayed", "arr_sum",
        "dep_count", "dep_delayed", "dep_sum"]


def new_stats():
    """Puste statystyki jednej grupy. arr_hist to liczba lotów dla każdej minuty opóźnienia."""
    stats = dict.fromkeys(SUMS, 0)
    stats["arr_hist"] = Counter()
    stats["causes"] = dict.fromkeys(CAUSES, 0.0)
    return stats


def group_keys(flight):
    """Zwraca klucze wszystkich grup, do których należy lot."""
    day = date.fromisoformat(flight["FlightDate"])
    keys = ["total|all",
            "carrier|" + flight["Reporting_Airline"],
            "month|" + flight["FlightDate"][:7],
            f"weekday|{day.isoweekday()}",  # 1 = poniedziałek, 7 = niedziela
            "airport|" + flight["Origin"]]
    if flight["CRSDepTime"]:
        hour = flight["CRSDepTime"][:2]
        keys.append("hour|" + ("00" if hour == "24" else hour))  # 2400 to północ
    return keys


def add_flight(stats, flight):
    """Dolicza jeden lot do statystyk grupy."""
    stats["flights"] += 1
    stats["cancelled"] += flight["Cancelled"] == "1"
    stats["diverted"] += flight["Diverted"] == "1"
    if flight["ArrDelay"]:
        delay = float(flight["ArrDelay"])
        stats["arr_count"] += 1
        stats["arr_delayed"] += delay >= DELAYED
        stats["arr_sum"] += delay
        stats["arr_hist"][round(delay)] += 1
    if flight["DepDelay"]:
        delay = float(flight["DepDelay"])
        stats["dep_count"] += 1
        stats["dep_delayed"] += delay >= DELAYED
        stats["dep_sum"] += delay
    for cause in CAUSES:
        if flight[cause]:
            stats["causes"][cause] += float(flight[cause])


def merge(total, part):
    """Dodaje częściowe statystyki z jednego mappera do sumy."""
    for name in SUMS:
        total[name] += part[name]
    for minute, count in part["arr_hist"].items():
        total["arr_hist"][int(minute)] += count  # w JSON klucze są tekstem
    for cause in CAUSES:
        total["causes"][cause] += part["causes"][cause]


def median(histogram):
    """Mediana z histogramu {wartość: liczba wystąpień}."""
    count = sum(histogram.values())
    if count == 0:
        return None
    # Dla parzystej liczby lotów mediana to średnia dwóch środkowych wartości.
    positions = [(count - 1) // 2, count // 2]
    values, seen = [], 0
    for value in sorted(histogram):
        seen += histogram[value]
        while positions and positions[0] < seen:
            values.append(value)
            positions.pop(0)
    return sum(values) / 2


def percent(part, whole):
    return round(100 * part / whole, 2) if whole else None


def mean(total, count):
    return round(total / count, 2) if count else None


def summary(stats):
    """Zamienia sumy na wskaźniki: odsetki, średnie, medianę i udział przyczyn opóźnień."""
    cause_total = sum(stats["causes"].values())
    return {
        "flights": stats["flights"],
        "cancelled_pct": percent(stats["cancelled"], stats["flights"]),
        "diverted_pct": percent(stats["diverted"], stats["flights"]),
        "arr_delayed_pct": percent(stats["arr_delayed"], stats["arr_count"]),
        "arr_delay_mean": mean(stats["arr_sum"], stats["arr_count"]),
        "arr_delay_median": median(stats["arr_hist"]),
        "dep_delayed_pct": percent(stats["dep_delayed"], stats["dep_count"]),
        "dep_delay_mean": mean(stats["dep_sum"], stats["dep_count"]),
        "cause_minutes": {cause: round(value) for cause, value in stats["causes"].items()},
        "cause_share_pct": {cause: percent(value, cause_total) for cause, value in stats["causes"].items()},
    }


def mapper():
    stats = defaultdict(new_stats)
    for row in csv.reader(sys.stdin):
        flight = dict(zip(COLUMNS, row))
        for key in group_keys(flight):
            add_flight(stats[key], flight)
    for key, group_stats in stats.items():
        print(key + "\t" + json.dumps(group_stats))


def reducer():
    # Hadoop sortuje linie po kluczu, więc częściowe wyniki tej samej grupy są obok siebie.
    pairs = (line.rstrip("\n").split("\t", 1) for line in sys.stdin)
    for key, group in groupby(pairs, key=lambda pair: pair[0]):
        total = new_stats()
        for _, value in group:
            merge(total, json.loads(value))
        print(key + "\t" + json.dumps(summary(total)))


if __name__ == "__main__":
    if sys.argv[1] == "map":
        mapper()
    else:
        reducer()
