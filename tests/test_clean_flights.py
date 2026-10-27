"""Reguly czyszczenia, CSV z przecinkami i fragmenty Hadoop bez naglowka."""

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
        writer.writerow(worker.COLUMNS + [""])
    for row in rows:
        writer.writerow([row[name] for name in worker.COLUMNS] + [""])
    return stream.getvalue()


def run_mapper(text, header=None):
    manifest = {"columns": header if header is not None else worker.COLUMNS + [""],
                "files": {"2024-01": {}}}
    out, err = io.StringIO(), io.StringIO()
    with patch.object(worker.Path, "read_text", return_value=json.dumps(manifest)), \
         patch.dict(worker.os.environ, {"mapreduce_map_input_file": "/flights/raw/2024-01.csv"}), \
         patch.object(sys, "stdin", io.StringIO(text)), redirect_stdout(out), redirect_stderr(err):
        worker.mapper()
    counts = Counter()
    for line in err.getvalue().splitlines():
        _, name, value = line.split(",")
        counts[name] += int(value)
    return list(csv.reader(io.StringIO(out.getvalue()))), counts


class CleaningTests(unittest.TestCase):
    def test_normalization_keeps_negative_delays_and_precision(self):
        counts = Counter()
        cleaned = worker.clean_row(flight(Origin=" lga ", ArrDelay=" -11.123456789 "), "2024-01", counts)
        self.assertEqual(cleaned["Origin"], "LGA")
        self.assertEqual(cleaned["ArrDelay"], "-11.123456789")
        self.assertEqual(cleaned["Cancelled"], "0")
        self.assertEqual(counts["trimmed_rows"], 1)

    def test_cancellations_diversions_and_missing_delays_stay(self):
        for changes, status in [({"Cancelled": "1"}, "cancelled"),
                                ({"Diverted": "1"}, "diverted"), ({}, "regular")]:
            with self.subTest(status=status):
                counts = Counter()
                cleaned = worker.clean_row(flight(ArrDelay="", **changes), "2024-01", counts)
                self.assertEqual(cleaned["ArrDelay"], "")
                self.assertEqual(counts["missing_arrival_" + status], 1)

    def test_bad_optional_values_become_missing_not_zero(self):
        counts = Counter()
        cleaned = worker.clean_row(flight(ArrDelay="NaN", DepDelay="inf", Distance="-1",
                                         AirTime="oops", CarrierDelay="0"), "2024-01", counts)
        for name in ["ArrDelay", "DepDelay", "Distance", "AirTime"]:
            self.assertEqual(cleaned[name], "")
            self.assertEqual(counts["invalid_" + name], 1)
            self.assertEqual(counts["missing_" + name], 0)
        self.assertEqual(cleaned["CarrierDelay"], "0")

    def test_invalid_core_fields_reject_row_once(self):
        cases = [("FlightDate", "2024-02-01", "date"), ("FlightDate", "2024-01-32", "date"),
                 ("Reporting_Airline", " ", "carrier"), ("Origin", "12A", "airport"),
                 ("OriginAirportID", "0", "airport_id"), ("Cancelled", "2", "status"),
                 ("Cancelled", "NaN", "status"), ("Diverted", "", "status")]
        for field, value, reason in cases:
            with self.subTest(field=field, value=value):
                counts = Counter()
                self.assertIsNone(worker.clean_row(flight(**{field: value}), "2024-01", counts))
                self.assertEqual(counts["rejected_" + reason], 1)

    def test_conflicting_statuses_reject_row(self):
        counts = Counter()
        self.assertIsNone(worker.clean_row(flight(Cancelled="1", Diverted="1"), "2024-01", counts))
        self.assertEqual(counts["rejected_status"], 1)

    def test_scheduled_time_validation(self):
        for value, expected in [("56", "0056"), ("2400", "2400"), ("2360", ""),
                                ("2500", ""), ("abc", ""), ("", "")]:
            with self.subTest(value=value):
                cleaned = worker.clean_row(flight(CRSDepTime=value), "2024-01", Counter())
                self.assertEqual(cleaned["CRSDepTime"], expected)

    def test_csv_and_headerless_splits(self):
        rows = [flight(), flight(FlightDate="2024-01-09", ArrDelay="")]
        combined, all_counts = run_mapper(csv_text(rows, header=True))
        first, a = run_mapper(csv_text(rows[:1], header=True))
        second, b = run_mapper(csv_text(rows[1:]))
        self.assertEqual(combined, first + second)
        self.assertEqual(all_counts, a + b)
        self.assertEqual(all_counts["records"], 2)
        self.assertEqual(all_counts["headers"], 1)
        self.assertEqual(combined[0][worker.COLUMNS.index("OriginCityName")], "New York, NY")

    def test_projection_uses_schema_positions_and_checks_empty_column_at_zero(self):
        header = ["", "Unused"] + list(reversed(worker.COLUMNS))
        original = flight()
        values = ["", "ignored"] + [original[name] for name in reversed(worker.COLUMNS)]
        for empty_value in ("", "unexpected"):
            with self.subTest(empty_value=empty_value):
                values[0] = empty_value
                stream = io.StringIO()
                writer = csv.writer(stream, lineterminator="\n")
                writer.writerow(header)
                writer.writerow(values)
                rows, counts = run_mapper(stream.getvalue(), header)
                if empty_value:
                    self.assertEqual(rows, [])
                    self.assertEqual(counts["rejected_structure"], 1)
                else:
                    expected = dict(original, Cancelled="0", Diverted="0")
                    self.assertEqual(rows, [[expected[name] for name in worker.COLUMNS]])
                    self.assertEqual(counts["written"], 1)

    def test_bad_structure_is_counted_and_not_emitted(self):
        rows, counts = run_mapper(csv_text([flight()], header=True) + '"unterminated\n1,2,3\n')
        self.assertEqual(len(rows), 1)
        self.assertEqual(counts["records"], 3)
        self.assertEqual(counts["written"], 1)
        self.assertEqual(counts["rejected_structure"], 2)

    def test_extra_unnamed_field_must_be_empty(self):
        text = csv_text([flight()]).rstrip("\n") + "not-empty\n"
        rows, counts = run_mapper(text)
        self.assertEqual(rows, [])
        self.assertEqual(counts["rejected_structure"], 1)

    def test_hadoop_counter_log_is_read_without_other_groups(self):
        text = "INFO done\n\tFlightQuality\n\t\trecords=25\n\t\twritten=24\n\tOther\n\t\trecords=999\n"
        self.assertEqual(read_counters(text), {"records": 25, "written": 24})


if __name__ == "__main__":
    unittest.main()
