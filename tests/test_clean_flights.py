"""Reguły czyszczenia, CSV z przecinkami w polach, fragmenty Hadoop bez nagłówka i duplikaty."""
import csv
import io
import json
import sys
import unittest
from collections import Counter
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import clean_flights as worker
from run_cleaning import read_counters

HEADER = worker.COLUMNS + [""]  # pliki BTS kończą się pustą kolumną


def flight(**changes):
    row = dict.fromkeys(worker.COLUMNS, "")
    row.update(FlightDate="2024-01-08", Reporting_Airline="9E",
               Flight_Number_Reporting_Airline="4801", OriginAirportID="12953",
               Origin="LGA", OriginCityName="New York, NY", DestAirportID="13871",
               Dest="OMA", DestCityName="Omaha, NE", CRSDepTime="0856",
               Cancelled="0.00", Diverted="0.00", ArrDelay="-11.00",
               DepDelay="-5.00", Distance="1148.00", AirTime="184.00")
    row.update(changes)
    return row


def csv_text(rows, header=False):
    stream = io.StringIO()
    writer = csv.writer(stream, lineterminator="\n")
    if header:
        writer.writerow(HEADER)
    for row in rows:
        writer.writerow([row[name] for name in worker.COLUMNS] + [""])
    return stream.getvalue()


def run(function, text):
    """Uruchamia mapper albo reducer tak jak Hadoop: dane na stdin, liczniki na stderr."""
    out, err = io.StringIO(), io.StringIO()
    with patch.object(worker.Path, "read_text", return_value=json.dumps({"columns": HEADER})), \
         patch.dict(worker.os.environ, {"mapreduce_map_input_file": "/flights/raw/2024-01.csv"}), \
         patch.object(sys, "stdin", io.StringIO(text)), redirect_stdout(out), redirect_stderr(err):
        function()
    counts = Counter()
    for line in err.getvalue().splitlines():
        _, name, value = line.split(",")
        counts[name] += int(value)
    return out.getvalue(), counts


def run_job(text):
    """Mapper -> sortowanie po kluczu (jak w Hadoop) -> reducer. Zwraca rekordy CSV i liczniki."""
    mapped, map_counts = run(worker.mapper, text)
    reduced, reduce_counts = run(worker.reducer, "".join(sorted(mapped.splitlines(keepends=True))))
    return list(csv.reader(io.StringIO(reduced))), map_counts + reduce_counts


class CleaningTests(unittest.TestCase):
    def test_normalization_keeps_negative_delays(self):
        cleaned = worker.clean_row(flight(Origin=" lga ", ArrDelay=" -11.5 ", CRSDepTime="56"),
                                   "2024-01", Counter())
        self.assertEqual(cleaned["Origin"], "LGA")
        self.assertEqual(cleaned["ArrDelay"], "-11.5")
        self.assertEqual(cleaned["CRSDepTime"], "0056")
        self.assertEqual((cleaned["Cancelled"], cleaned["Diverted"]), ("0", "0"))

    def test_cancelled_and_diverted_flights_without_arrival_are_kept(self):
        for changes, status in [({"Cancelled": "1.00"}, "cancelled"), ({"Diverted": "1"}, "diverted")]:
            with self.subTest(status=status):
                counts = Counter()
                cleaned = worker.clean_row(flight(ArrDelay="", **changes), "2024-01", counts)
                self.assertEqual(cleaned["ArrDelay"], "")
                self.assertEqual(counts[status], 1)

    def test_regular_flight_without_arrival_is_rejected(self):
        counts = Counter()
        self.assertIsNone(worker.clean_row(flight(ArrDelay=""), "2024-01", counts))
        self.assertEqual(counts, {"rejected_no_arrival": 1})

    def test_bad_optional_values_become_missing_not_zero(self):
        counts = Counter()
        cleaned = worker.clean_row(flight(ArrDelay="NaN", DepDelay="inf", Distance="-1",
                                          AirTime="oops", CarrierDelay="0"), "2024-01", counts)
        for name in ["ArrDelay", "DepDelay", "Distance", "AirTime"]:
            self.assertEqual(cleaned[name], "")
            self.assertEqual(counts["invalid_" + name], 1)
        self.assertEqual(cleaned["CarrierDelay"], "0")

    def test_implausible_air_time_becomes_missing(self):
        # 1148 mil: 184 min to ok. 374 mph; 30 min to ponad 2000 mph, 0 min jest niemożliwe.
        for air_time, expected in [("184.00", "184.00"), ("30.00", ""), ("0.00", "")]:
            with self.subTest(air_time=air_time):
                counts = Counter()
                cleaned = worker.clean_row(flight(AirTime=air_time), "2024-01", counts)
                self.assertEqual(cleaned["AirTime"], expected)
                self.assertEqual(counts["implausible_AirTime"], int(expected == ""))

    def test_extreme_delay_is_counted_but_kept(self):
        counts = Counter()
        cleaned = worker.clean_row(flight(ArrDelay="1500.00"), "2024-01", counts)
        self.assertEqual(cleaned["ArrDelay"], "1500.00")
        self.assertEqual(counts["extreme_ArrDelay"], 1)

    def test_scheduled_time_validation(self):
        for value, expected in [("2400", "2400"), ("2360", ""), ("2500", ""), ("abc", ""), ("", "")]:
            with self.subTest(value=value):
                cleaned = worker.clean_row(flight(CRSDepTime=value), "2024-01", Counter())
                self.assertEqual(cleaned["CRSDepTime"], expected)

    def test_invalid_core_fields_reject_row(self):
        cases = [("FlightDate", "2024-02-01", "date"), ("FlightDate", "2024-01-32", "date"),
                 ("FlightDate", "20240108", "date"), ("Reporting_Airline", " ", "carrier"),
                 ("Origin", "12A", "airport"), ("OriginAirportID", "x1", "airport"),
                 ("Cancelled", "2", "status"), ("Cancelled", "NaN", "status"),
                 ("Diverted", "", "status")]
        for field, value, reason in cases:
            with self.subTest(field=field, value=value):
                counts = Counter()
                self.assertIsNone(worker.clean_row(flight(**{field: value}), "2024-01", counts))
                self.assertEqual(counts, {"rejected_" + reason: 1})

    def test_cancelled_and_diverted_at_once_is_rejected(self):
        counts = Counter()
        self.assertIsNone(worker.clean_row(flight(Cancelled="1", Diverted="1"), "2024-01", counts))
        self.assertEqual(counts["rejected_status"], 1)

    def test_split_without_header_gives_same_result(self):
        rows = [flight(), flight(FlightDate="2024-01-09", ArrDelay="3.00")]
        combined, all_counts = run(worker.mapper, csv_text(rows, header=True))
        first, a = run(worker.mapper, csv_text(rows[:1], header=True))
        second, b = run(worker.mapper, csv_text(rows[1:]))
        self.assertEqual(combined, first + second)
        self.assertEqual(all_counts, a + b)
        self.assertEqual((all_counts["records"], all_counts["headers"]), (2, 1))
        self.assertEqual(all_counts["month_2024-01"], 2)

    def test_bad_structure_is_counted_and_not_written(self):
        rows, counts = run_job(csv_text([flight()], header=True) + '"unterminated\n1,2,3\n')
        self.assertEqual(len(rows), 1)
        self.assertEqual((counts["records"], counts["written"], counts["rejected_structure"]), (3, 1, 2))

    def test_duplicates_are_removed_by_reducer(self):
        rows = [flight(), flight(FlightDate="2024-01-09"), flight(DepDelay="99.00")]
        written, counts = run_job(csv_text(rows, header=True))
        self.assertEqual((counts["valid"], counts["written"], counts["duplicates"]), (3, 2, 1))
        self.assertEqual(len(written), 2)
        self.assertEqual(written[0][worker.COLUMNS.index("OriginCityName")], "New York, NY")
        self.assertEqual(len(written[0]), len(worker.COLUMNS))

    def test_counters_are_read_only_from_flight_group(self):
        log = "INFO done\n\tFlightQuality\n\t\trecords=25\n\t\tmonth_2024-01=25\n\tOther\n\t\trecords=9\n"
        self.assertEqual(read_counters(log), {"records": 25, "month_2024-01": 25})


if __name__ == "__main__":
    unittest.main()
