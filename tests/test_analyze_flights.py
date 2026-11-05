"""Statystyki analiz: grupy lotu, odsetki, mediana i łączenie wyników z wielu mapperów."""
import csv
import io
import json
import sys
import unittest
from collections import Counter
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import analyze_flights as analysis
from clean_flights import COLUMNS


def flight(**changes):
    row = dict.fromkeys(COLUMNS, "")
    row.update(FlightDate="2024-01-08", Reporting_Airline="9E", Origin="LGA", Dest="OMA",
               CRSDepTime="0856", Cancelled="0", Diverted="0", DepDelay="-5.00", ArrDelay="-11.00")
    row.update(changes)
    return row


def csv_text(rows):
    stream = io.StringIO()
    csv.writer(stream, lineterminator="\n").writerows([row[name] for name in COLUMNS] for row in rows)
    return stream.getvalue()


def run(function, text):
    out = io.StringIO()
    with patch.object(sys, "stdin", io.StringIO(text)), redirect_stdout(out):
        function()
    return out.getvalue()


def run_job(*splits):
    """Każdy fragment danych przetwarza osobny mapper; reducer dostaje posortowane wyniki."""
    mapped = "".join(run(analysis.mapper, csv_text(rows)) for rows in splits)
    reduced = run(analysis.reducer, "".join(sorted(mapped.splitlines(keepends=True))))
    results = {}
    for line in reduced.splitlines():
        key, value = line.split("\t", 1)
        results[key] = json.loads(value)
    return results


class AnalysisTests(unittest.TestCase):
    def test_flight_belongs_to_all_groups(self):
        keys = analysis.group_keys(flight())
        self.assertEqual(keys, ["total|all", "carrier|9E", "month|2024-01", "weekday|1",
                                "airport|LGA", "hour|08"])

    def test_midnight_2400_is_hour_zero(self):
        self.assertIn("hour|00", analysis.group_keys(flight(CRSDepTime="2400")))

    def test_median_with_odd_and_even_count(self):
        self.assertEqual(analysis.median(Counter({-5: 1, 3: 1, 10: 1})), 3)
        self.assertEqual(analysis.median(Counter({-5: 1, 3: 1, 10: 1, 20: 1})), 6.5)
        self.assertEqual(analysis.median(Counter({0: 3, 100: 1})), 0)
        self.assertIsNone(analysis.median(Counter()))

    def test_statistics_of_a_group(self):
        rows = [flight(ArrDelay="-10.00"), flight(ArrDelay="20.00", DepDelay="25.00",
                                                   CarrierDelay="5.00", WeatherDelay="15.00"),
                flight(ArrDelay="", DepDelay="", Cancelled="1")]
        stats = run_job(rows)["carrier|9E"]
        self.assertEqual(stats["flights"], 3)
        self.assertEqual(stats["cancelled_pct"], 33.33)
        self.assertEqual(stats["arr_delayed_pct"], 50.0)  # 1 z 2 lotów, które doleciały
        self.assertEqual(stats["arr_delay_mean"], 5.0)
        self.assertEqual(stats["arr_delay_median"], 5.0)
        self.assertEqual(stats["dep_delayed_pct"], 50.0)
        self.assertEqual(stats["cause_share_pct"]["WeatherDelay"], 75.0)

    def test_results_do_not_depend_on_split_into_mappers(self):
        rows = [flight(ArrDelay=str(d), Reporting_Airline=c, FlightDate=f"2024-01-{day:02d}")
                for d, c, day in [(-3, "AA", 1), (40, "AA", 2), (7, "DL", 3), (15, "AA", 4), (0, "DL", 5)]]
        self.assertEqual(run_job(rows), run_job(rows[:2], rows[2:4], rows[4:]))

    def test_total_counts_every_flight_once(self):
        rows = [flight(), flight(Reporting_Airline="AA", Origin="JFK")]
        results = run_job(rows)
        self.assertEqual(results["total|all"]["flights"], 2)
        self.assertEqual(results["carrier|AA"]["flights"], 1)
        self.assertEqual(results["airport|LGA"]["flights"], 1)


if __name__ == "__main__":
    unittest.main()
