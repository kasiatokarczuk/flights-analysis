"""Parsowanie tabel Wikipedii: wiersze stanów, przypisy, scalone komórki i dopasowanie kodów."""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import scrape_wikipedia as scraper

AIRPORTS_HTML = """
<table class="wikitable sortable">
<tr><th>City</th><th>FAA</th><th>IATA</th><th>ICAO</th><th>Airport</th><th>Role</th><th>Enplanements (2019)</th></tr>
<tr><th colspan="7">ALABAMA</th></tr>
<tr><td>Birmingham</td><td>BHM</td><td>BHM</td><td>KBHM</td><td>Birmingham–Shuttlesworth International Airport<sup>[1]</sup></td><td>P-S</td><td>1,596,510</td></tr>
<tr><td>Dothan</td><td>DHN</td><td>DHN</td><td>KDHN</td><td>Dothan Regional Airport</td><td>P-N</td><td>39,339</td></tr>
<tr><th colspan="7">NORTH CAROLINA</th></tr>
<tr><td>West Palm Beach</td><td>DJT</td><td>DJT</td><td>KDJT</td><td>Palm Beach Airport</td><td>P-M</td><td>4,127,211</td></tr>
<tr><td>Hilo <a>,</a> Hawaii</td><td>ITO</td><td>ITO</td><td>PHTO</td><td>Hilo International Airport</td><td>P-S</td><td>1</td></tr>
<tr><td>Charlotte</td><td>CLT</td><td>CLT</td><td>KCLT</td><td>Charlotte Douglas International Airport</td><td>P-L</td><td>24,199,688</td></tr>
<tr><td>Smallville</td><td>SV1</td><td></td><td></td><td>Smallville Field</td><td>GA</td><td></td></tr>
</table>
"""

CARRIERS_HTML = """
<h2>Mainline carriers</h2>
<table class="wikitable">
<tr><th>Airline</th><th>Image</th><th>IATA</th><th>ICAO</th><th>Callsign</th><th>Notes</th></tr>
<tr><td>Alaska Airlines</td><td></td><td>AS</td><td>ASA</td><td>ALASKA</td><td></td></tr>
<tr><td>Hawaiian Airlines</td><td></td><td>AS</td><td>HAL</td><td>HAWAIIAN</td><td>Merged with Alaska.</td></tr>
<tr><td>Delta Air Lines</td><td></td><td>DL</td><td>DAL</td><td>DELTA</td><td></td></tr>
</table>
<h2>Regional carriers</h2>
<table class="wikitable">
<tr><th>Airline</th><th>IATA</th><th>ICAO</th><th>Callsign</th><th>Notes</th></tr>
<tr><td rowspan="2">Endeavor Air</td><td rowspan="2">9E</td><td>EDV</td><td>ENDEAVOR</td><td>Operates as Delta Connection.</td></tr>
<tr><td>FLG</td><td>FLAGSHIP</td><td>Former code.</td></tr>
</table>
<h2>Cargo carriers</h2>
<table class="wikitable">
<tr><th>Airline</th><th>IATA</th><th>ICAO</th></tr>
<tr><td>Some Cargo</td><td>DL</td><td>XXX</td></tr>
</table>
"""


class ScraperTests(unittest.TestCase):
    def test_airports_get_state_from_header_rows(self):
        airports = scraper.parse_airports(AIRPORTS_HTML)
        self.assertEqual([a["iata"] for a in airports], ["BHM", "DHN", "DJT", "ITO", "CLT", None])
        self.assertEqual([a["state"] for a in airports], ["Alabama"] * 2 + ["North Carolina"] * 4)

    def test_airport_fields_are_cleaned(self):
        bhm = scraper.parse_airports(AIRPORTS_HTML)[0]
        self.assertEqual(bhm["name"], "Birmingham–Shuttlesworth International Airport")  # bez [1]
        self.assertEqual(bhm["enplanements_2019"], 1596510)
        self.assertEqual((bhm["role"], bhm["role_group"]), ("P-S", "małe"))

    def test_wikipedia_code_is_translated_to_bts_code(self):
        airports = {a["iata"]: a for a in scraper.parse_airports(AIRPORTS_HTML)}
        self.assertEqual(airports["DJT"]["bts_code"], "PBI")
        self.assertEqual(airports["BHM"]["bts_code"], "BHM")

    def test_space_before_comma_is_removed(self):
        ito = next(a for a in scraper.parse_airports(AIRPORTS_HTML) if a["iata"] == "ITO")
        self.assertEqual(ito["city"], "Hilo, Hawaii")

    def test_airport_without_primary_role_is_kept(self):
        field = scraper.parse_airports(AIRPORTS_HTML)[-1]
        self.assertEqual((field["role"], field["role_group"], field["enplanements_2019"]),
                         ("GA", "inne / brak", None))

    def test_rowspan_repeats_cells_in_next_rows(self):
        carriers = scraper.parse_carriers(CARRIERS_HTML)
        endeavor = [c for c in carriers if c["name"] == "Endeavor Air"]
        self.assertEqual([(c["iata"], c["icao"]) for c in endeavor], [("9E", "EDV"), ("9E", "FLG")])

    def test_carrier_type_and_brand(self):
        carriers = scraper.parse_carriers(CARRIERS_HTML)
        endeavor = next(c for c in carriers if c["icao"] == "EDV")
        self.assertEqual((endeavor["type"], endeavor["operates_as"]), ("regional", "Delta Connection"))
        self.assertEqual(next(c for c in carriers if c["name"] == "Some Cargo")["type"], "cargo")

    def test_matching_prefers_passenger_carriers_and_fills_missing(self):
        carriers = scraper.parse_carriers(CARRIERS_HTML)
        matched, notes = scraper.match_carriers(carriers, ["AS", "DL", "9E", "HA", "ZZ"])
        self.assertEqual(matched["DL"]["name"], "Delta Air Lines")  # nie przewoźnik cargo
        self.assertEqual(matched["AS"]["name"], "Alaska Airlines")
        self.assertEqual((matched["HA"]["name"], matched["HA"]["source"]), ("Hawaiian Airlines", "manual"))
        self.assertNotIn("ZZ", matched)
        self.assertTrue(any(note.startswith("AS: kilku") for note in notes))
        self.assertTrue(any(note.startswith("ZZ: brak") for note in notes))

    def test_quality_check_finds_duplicates(self):
        airports = scraper.parse_airports(AIRPORTS_HTML)
        self.assertEqual(scraper.check_airports(airports), [])
        problems = scraper.check_airports(airports + [dict(airports[0])])
        self.assertTrue(any("BHM" in problem for problem in problems))


if __name__ == "__main__":
    unittest.main()
