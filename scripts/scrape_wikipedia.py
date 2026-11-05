"""Web scraping: dane o lotniskach i przewoźnikach z tabel Wikipedii.

    python scripts/scrape_wikipedia.py            # użyj zapisanych stron, jeśli są
    python scripts/scrape_wikipedia.py --refresh  # pobierz strony od nowa

Pobrane strony HTML zapisujemy w data/scraped/, żeby ponowne uruchomienie nie odpytywało
Wikipedii. Wyniki: results/airports.json, results/carriers.json i raport results/scraping.md
z dopasowaniem do kodów lotnisk i przewoźników z naszych analiz.
"""
import argparse
import json
import re
import sys
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

from common import ROOT, RESULTS

SCRAPED = ROOT / "data" / "scraped"
PAGES = {
    "airports": "https://en.wikipedia.org/wiki/List_of_airports_in_the_United_States",
    "carriers": "https://en.wikipedia.org/wiki/List_of_airlines_of_the_United_States",
}
# Wikipedia prosi, żeby automatyczne zapytania przedstawiały się nazwą programu.
HEADERS = {"User-Agent": "BigDataFlightsProject/1.0 (student project, Python requests)"}

ROLE_GROUPS = {"P-L": "duże", "P-M": "średnie", "P-S": "małe", "P-N": "bardzo małe"}
# Lotniska, które Wikipedia podaje pod innym kodem niż dane BTS (kod z Wikipedii -> kod BTS).
# West Palm Beach: Wikipedia podaje nową nazwę i kod DJT, w danych lotów z lat 2024-2025 jest PBI.
BTS_CODES = {"DJT": "PBI"}
# Przewoźnicy z naszych danych, których nie ma w tabeli Wikipedii pod tym kodem.
# Uzupełniamy ich ręcznie i oznaczamy jako źródło "manual".
MANUAL_CARRIERS = {
    "HA": {"name": "Hawaiian Airlines", "type": "mainline"},
    "NK": {"name": "Spirit Airlines", "type": "mainline"},
}


def fetch(name, refresh):
    """Zwraca HTML strony: z zapisanej kopii albo pobrany z Wikipedii."""
    path = SCRAPED / f"{name}.html"
    if path.exists() and not refresh:
        print(f"{name}: używam zapisanej strony {path.relative_to(ROOT)}")
        return path.read_text(encoding="utf-8")
    print(f"{name}: pobieranie {PAGES[name]}")
    response = requests.get(PAGES[name], headers=HEADERS, timeout=60)
    response.raise_for_status()
    SCRAPED.mkdir(parents=True, exist_ok=True)
    path.write_text(response.text, encoding="utf-8")
    meta = {"url": PAGES[name], "fetched_utc": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    (SCRAPED / f"{name}.json").write_text(json.dumps(meta, indent=2) + "\n", encoding="utf-8")
    return response.text


def cell_text(cell):
    """Tekst komórki bez przypisów typu [1] i bez nadmiarowych spacji."""
    for footnote in cell.select("sup"):
        footnote.decompose()
    text = " ".join(cell.get_text(" ").split())
    return re.sub(r" ([,;:])", r"\1", text)  # "Hilo , Hawaii" -> "Hilo, Hawaii"


def table_rows(table):
    """Zamienia tabelę HTML na listę wierszy (list tekstów).

    Uwzględnia scalone komórki: komórka z colspan zajmuje kilka kolumn, a z rowspan
    powtarza się w kolejnych wierszach - tak jak widać to na stronie.
    """
    rows, carried = [], {}  # carried: kolumna -> (tekst, ile wierszy jeszcze)
    for tr in table.find_all("tr"):
        row, column = [], 0
        cells = tr.find_all(["th", "td"], recursive=False)
        while cells or column in carried:
            if column in carried:
                text, left = carried.pop(column)
                if left > 1:
                    carried[column] = (text, left - 1)
                row.append(text)
                column += 1
                continue
            cell = cells.pop(0)
            text = cell_text(cell)
            for _ in range(int(cell.get("colspan", 1))):
                if int(cell.get("rowspan", 1)) > 1:
                    carried[column] = (text, int(cell["rowspan"]) - 1)
                row.append(text)
                column += 1
        rows.append(row)
    return rows


def find_column(header, name, required=True):
    """Numer kolumny, której nagłówek zaczyna się od podanej nazwy (None, gdy jej nie ma)."""
    for index, title in enumerate(header):
        if title.lower().startswith(name.lower()):
            return index
    if required:
        raise ValueError(f"Brak kolumny {name} w tabeli Wikipedii.")
    return None


def get(row, index):
    """Wartość z kolumny albo None, gdy kolumny nie ma lub komórka jest pusta."""
    return (row[index] or None) if index is not None and index < len(row) else None


def as_int(text):
    digits = text.replace(",", "").strip()
    return int(digits) if digits.isdigit() else None


def parse_airports(html):
    """Lotniska z tabeli z kolumnami IATA i Role. Wiersze z samą nazwą stanu to nagłówki stanów."""
    soup = BeautifulSoup(html, "html.parser")
    airports, state = [], None
    for table in soup.select("table.wikitable"):
        rows = table_rows(table)
        header = rows[0]
        if find_column(header, "IATA", False) is None or find_column(header, "Role", False) is None:
            continue
        col = {name: find_column(header, name) for name in
               ("City", "FAA", "IATA", "ICAO", "Airport", "Role", "Enplanements")}
        for row in rows[1:]:
            values = {value for value in row if value}
            if len(values) == 1:  # np. "ALABAMA" w całym wierszu
                state = values.pop().title()
                continue
            role, iata = get(row, col["Role"]), get(row, col["IATA"])
            airports.append({
                "iata": iata, "bts_code": BTS_CODES.get(iata, iata), "faa": get(row, col["FAA"]),
                "icao": get(row, col["ICAO"]), "name": get(row, col["Airport"]),
                "city": get(row, col["City"]), "state": state, "role": role,
                "role_group": ROLE_GROUPS.get(role, "inne / brak"),
                "enplanements_2019": as_int(get(row, col["Enplanements"]) or ""),
            })
    if not airports:
        raise ValueError("Nie znaleziono tabeli lotnisk na stronie Wikipedii.")
    return airports


def carrier_type(section):
    """Typ przewoźnika na podstawie nagłówka sekcji, pod którą jest tabela."""
    section = section.lower()
    for word, kind in [("mainline", "mainline"), ("major", "mainline"), ("regional", "regional"),
                       ("cargo", "cargo"), ("charter", "charter")]:
        if word in section:
            return kind
    return "other"


def parse_carriers(html):
    """Przewoźnicy ze wszystkich tabel z kolumną IATA, z typem wziętym z nagłówka sekcji."""
    soup = BeautifulSoup(html, "html.parser")
    carriers = []
    for table in soup.select("table.wikitable"):
        rows = table_rows(table)
        header = rows[0]
        if find_column(header, "IATA", False) is None:
            continue
        heading = table.find_previous(["h2", "h3", "h4"])
        section = heading.get_text(" ", strip=True) if heading else ""
        col = {"Airline": find_column(header, "Airline"), "IATA": find_column(header, "IATA")}
        for name in ("ICAO", "Callsign", "Notes"):
            col[name] = find_column(header, name, required=False)
        for row in rows[1:]:
            note = get(row, col["Notes"]) or ""
            brand = re.search(r"[Oo]perates as ([A-Z][\w.&' -]+?)(?:[.,;(]|$| and )", note)
            carriers.append({
                "iata": get(row, col["IATA"]), "icao": get(row, col["ICAO"]),
                "name": get(row, col["Airline"]), "callsign": get(row, col["Callsign"]),
                "section": section, "type": carrier_type(section),
                "operates_as": brand[1].strip() if brand else None, "source": "wikipedia",
            })
    if not carriers:
        raise ValueError("Nie znaleziono tabel przewoźników na stronie Wikipedii.")
    return carriers


def match_carriers(carriers, codes):
    """Wybiera jednego przewoźnika dla każdego kodu z naszych danych.

    Pierwszeństwo mają linie główne i regionalne (kody IATA bywają powtórnie używane
    np. przez przewoźników cargo). Brakujące kody uzupełniamy z MANUAL_CARRIERS.
    """
    matched, notes = {}, []
    for code in sorted(codes):
        candidates = [c for c in carriers if c["iata"] == code and c["type"] in ("mainline", "regional")]
        names = list(dict.fromkeys(c["name"] for c in candidates))  # bez powtórzeń, w kolejności
        if len(names) > 1:
            notes.append(f"{code}: kilku kandydatów ({', '.join(names)}), wybrano pierwszego")
        if candidates:
            matched[code] = candidates[0]
        elif code in MANUAL_CARRIERS:
            matched[code] = {"iata": code, **MANUAL_CARRIERS[code], "source": "manual"}
            notes.append(f"{code}: brak w tabeli Wikipedii, uzupełniono ręcznie ({matched[code]['name']})")
        else:
            notes.append(f"{code}: brak w tabeli Wikipedii")
    return matched, notes


def check_airports(airports):
    """Zwraca listę problemów z jakością danych o lotniskach."""
    problems = []
    codes = [a["iata"] for a in airports if a["iata"]]
    bad = [code for code in codes if not re.fullmatch(r"[A-Z0-9]{3}", code)]
    if bad:
        problems.append(f"Niepoprawne kody IATA: {', '.join(bad[:10])}")
    duplicates = sorted({code for code in codes if codes.count(code) > 1})
    if duplicates:
        problems.append(f"Powtórzone kody IATA: {', '.join(duplicates)}")
    if any(a["state"] is None for a in airports):
        problems.append("Część lotnisk nie ma przypisanego stanu.")
    return problems


def save_report(airports, carriers, matched, carrier_notes, analysis):
    """Raport: ile rekordów, problemy jakości i dopasowanie do kodów z analiz."""
    our_airports = set(analysis["airport"])
    by_code = {a["bts_code"]: a for a in airports if a["bts_code"]}
    found = our_airports & set(by_code)
    groups = {}
    for code in found:
        group = by_code[code]["role_group"]
        groups[group] = groups.get(group, 0) + 1
    lines = ["# Raport ze scrapingu Wikipedii", "",
             f"- Lotniska: [{PAGES['airports']}]({PAGES['airports']}), liczba wierszy: {len(airports)}",
             f"- Przewoźnicy: [{PAGES['carriers']}]({PAGES['carriers']}), liczba wierszy: {len(carriers)}", "",
             "## Jakość danych", ""]
    lines += [f"- {p}" for p in check_airports(airports)] or ["- Brak problemów z kodami lotnisk."]
    lines += ["", "## Dopasowanie lotnisk do naszych danych", "",
              f"Znaleziono {len(found)} z {len(our_airports)} lotnisk z analiz.", "",
              "| Wielkość (kategoria FAA) | Lotniska |", "|---|---:|"]
    for group in list(ROLE_GROUPS.values()) + ["inne / brak"]:
        lines.append(f"| {group} | {groups.get(group, 0)} |")
    missing = sorted(our_airports - found)
    lines += ["", f"Brak w tabeli Wikipedii ({len(missing)}): {', '.join(missing) or '–'}", "",
              "Kody różne na Wikipedii i w danych BTS (Wikipedia → BTS): "
              + ", ".join(f"{wiki} → {bts}" for wiki, bts in BTS_CODES.items()), "",
              "## Dopasowanie przewoźników", "",
              "| Kod | Nazwa | Typ | Lata jako | Źródło |", "|---|---|---|---|---|"]
    for code, c in sorted(matched.items()):
        lines.append(f"| {code} | {c['name']} | {c['type']} | {c.get('operates_as') or '–'} | {c['source']} |")
    lines += [""] + [f"- {note}" for note in carrier_notes] + [""]
    (RESULTS / "scraping.md").write_text("\n".join(lines), encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description="Scraping tabel Wikipedii o lotniskach i przewoźnikach.")
    parser.add_argument("--refresh", action="store_true", help="pobierz strony od nowa")
    args = parser.parse_args()

    airports = parse_airports(fetch("airports", args.refresh))
    carriers = parse_carriers(fetch("carriers", args.refresh))
    analysis = json.loads((RESULTS / "analysis.json").read_text(encoding="utf-8"))["results"]
    matched, carrier_notes = match_carriers(carriers, analysis["carrier"])

    (RESULTS / "airports.json").write_text(json.dumps(airports, indent=2, ensure_ascii=False) + "\n",
                                           encoding="utf-8")
    (RESULTS / "carriers.json").write_text(json.dumps(list(matched.values()), indent=2, ensure_ascii=False)
                                           + "\n", encoding="utf-8")
    save_report(airports, carriers, matched, carrier_notes, analysis)
    print(f"GOTOWE. Lotniska: {len(airports)}, przewoźnicy: {len(matched)}. Raport: results/scraping.md")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit("\nPrzerwano.")
    except (OSError, ValueError, requests.RequestException) as error:
        sys.exit(f"BŁĄD: {error}")
