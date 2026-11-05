"""Uruchamia analizy statystyczne w Hadoop i zapisuje wyniki.

    python scripts/run_analysis.py

Wejście: /flights/clean (wynik czyszczenia). Wynik w HDFS: /flights/analysis.
Mapper i reducer: scripts/analyze_flights.py. Lokalnie powstają:
results/analysis.json (wszystkie wyniki) i results/analysis.md (tabele do czytania).
"""
import json
import sys
from datetime import datetime, timezone

from analyze_flights import CAUSES
from common import RESULTS, application_id, hadoop, hdfs_exists, number, run_streaming

INPUT, OUTPUT = "/flights/clean", "/flights/analysis"
WEEKDAYS = {"1": "poniedziałek", "2": "wtorek", "3": "środa", "4": "czwartek",
            "5": "piątek", "6": "sobota", "7": "niedziela"}
CAUSE_NAMES = {"CarrierDelay": "przewoźnik", "WeatherDelay": "pogoda",
               "NASDelay": "system kontroli ruchu (NAS)", "SecurityDelay": "kontrola bezpieczeństwa",
               "LateAircraftDelay": "spóźniony samolot z poprzedniego lotu"}
TOP_AIRPORTS = 20


def load_results():
    """Czyta wynik zadania z HDFS: {grupa: {klucz: statystyki}}, np. results["carrier"]["WN"]."""
    results = {}
    for line in hadoop("hdfs", "dfs", "-cat", OUTPUT + "/part-*").stdout.splitlines():
        key, value = line.split("\t", 1)
        group, name = key.split("|", 1)
        results.setdefault(group, {})[name] = json.loads(value)
    return results


def fmt(value, digits=1):
    """Liczba w zapisie polskim (przecinek dziesiętny); brak wartości jako '–'."""
    if value is None:
        return "–"
    return f"{value:.{digits}f}".replace(".", ",")


def on_time(stats):
    """Odsetek lotów, które doleciały z opóźnieniem mniejszym niż 15 minut."""
    return None if stats["arr_delayed_pct"] is None else 100 - stats["arr_delayed_pct"]


def table(headers, rows):
    """Tabela Markdown; pierwsza kolumna do lewej, pozostałe (liczby) do prawej."""
    lines = ["| " + " | ".join(headers) + " |", "|---" + "|---:" * (len(headers) - 1) + "|"]
    lines += ["| " + " | ".join(str(cell) for cell in row) + " |" for row in rows]
    return lines + [""]


def delay_row(label, stats):
    return [label, number(stats["flights"]), fmt(on_time(stats)) + "%",
            fmt(stats["arr_delay_mean"]), fmt(stats["arr_delay_median"]),
            fmt(stats["cancelled_pct"], 2) + "%"]


def save_report(results, application):
    """Zapisuje wyniki jako JSON i jako raport Markdown z tabelami pięciu analiz."""
    report = {"finished_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
              "application_id": application, "hdfs_input": INPUT, "hdfs_output": OUTPUT,
              "results": results}
    (RESULTS / "analysis.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

    total = results["total"]["all"]
    delay_headers = ["Loty", "Na czas", "Śr. opóźnienie [min]", "Mediana [min]", "Odwołane"]
    lines = [
        "# Wyniki analiz", "",
        f"Zadanie YARN: `{application}`. Wejście: `{INPUT}`, wynik: `{OUTPUT}` w HDFS.", "",
        f"Wszystkich lotów: **{number(total['flights'])}**. Na czas doleciało **{fmt(on_time(total))}%**, "
        f"średnie opóźnienie przylotu to **{fmt(total['arr_delay_mean'])} min** "
        f"(mediana {fmt(total['arr_delay_median'])} min), odwołano **{fmt(total['cancelled_pct'], 2)}%** lotów.", "",
        "„Na czas” oznacza przylot opóźniony o mniej niż 15 minut (definicja BTS), liczony wśród lotów, "
        "które doleciały. Ujemne opóźnienie to przylot przed czasem, dlatego mediana bywa ujemna.", "",
        "## 1. Punktualność przewoźników", "",
    ]
    carriers = sorted(results["carrier"].items(), key=lambda item: -on_time(item[1]))
    lines += table(["Przewoźnik"] + delay_headers, [delay_row(code, s) for code, s in carriers])

    lines += ["## 2. Sezonowość", "", "### Miesiące", ""]
    lines += table(["Miesiąc"] + delay_headers,
                   [delay_row(month, s) for month, s in sorted(results["month"].items())])
    lines += ["### Dni tygodnia", ""]
    lines += table(["Dzień"] + delay_headers,
                   [delay_row(WEEKDAYS[day], s) for day, s in sorted(results["weekday"].items())])

    lines += ["## 3. Godzina planowanego odlotu", ""]
    lines += table(["Godzina", "Loty", "Opóźnione odloty", "Śr. opóźnienie odlotu [min]",
                    "Opóźnione przyloty"],
                   [[f"{hour}:00", number(s["flights"]), fmt(s["dep_delayed_pct"]) + "%",
                     fmt(s["dep_delay_mean"]), fmt(s["arr_delayed_pct"]) + "%"]
                    for hour, s in sorted(results["hour"].items())])

    lines += ["## 4. Przyczyny opóźnień", "",
              "Minuty opóźnienia przypisane przez przewoźników do pięciu przyczyn "
              "(raportowane tylko dla przylotów opóźnionych o co najmniej 15 minut).", ""]
    lines += table(["Przyczyna", "Minuty", "Udział"],
                   [[CAUSE_NAMES[c], number(total["cause_minutes"][c]),
                     fmt(total["cause_share_pct"][c]) + "%"] for c in CAUSES])
    lines += ["Udział przyczyn u poszczególnych przewoźników:", ""]
    lines += table(["Przewoźnik"] + [CAUSE_NAMES[c] for c in CAUSES],
                   [[code] + [fmt(s["cause_share_pct"][c]) + "%" for c in CAUSES]
                    for code, s in sorted(results["carrier"].items())])

    lines += [f"## 5. Lotniska ({TOP_AIRPORTS} z największą liczbą odlotów)", "",
              f"Pełna lista ({len(results['airport'])} lotnisk) jest w `results/analysis.json`.", ""]
    airports = sorted(results["airport"].items(), key=lambda item: -item[1]["flights"])[:TOP_AIRPORTS]
    lines += table(["Lotnisko", "Odloty", "Opóźnione odloty", "Śr. opóźnienie odlotu [min]", "Odwołane"],
                   [[code, number(s["flights"]), fmt(s["dep_delayed_pct"]) + "%",
                     fmt(s["dep_delay_mean"]), fmt(s["cancelled_pct"], 2) + "%"] for code, s in airports])
    (RESULTS / "analysis.md").write_text("\n".join(lines), encoding="utf-8")


def main():
    if not hdfs_exists(INPUT + "/_SUCCESS"):
        sys.exit(f"Brak oczyszczonych danych w {INPUT}. Najpierw uruchom scripts/run_cleaning.py.")
    # Istniejącego wyniku nie nadpisujemy - trzeba go usunąć ręcznie.
    if hdfs_exists(OUTPUT):
        sys.exit(f"Katalog {OUTPUT} już istnieje w HDFS. Aby powtórzyć analizy, usuń go:\n"
                 f"  docker compose exec resourcemanager hdfs dfs -rm -r {OUTPUT}")

    print(f"Analizy w Hadoop. Wejście: {INPUT}, wynik: {OUTPUT}")
    log = run_streaming("flights-analysis", [INPUT], OUTPUT, "scripts/analyze_flights.py",
                        files=["scripts/clean_flights.py"], log_name="analysis.log")
    results = load_results()

    # Każdy oczyszczony lot musi trafić do sumy całego zbioru.
    cleaning = json.loads((RESULTS / "data_quality.json").read_text(encoding="utf-8"))
    if results["total"]["all"]["flights"] != cleaning["counters"]["written"]:
        raise ValueError("Liczba lotów w analizie nie zgadza się z wynikiem czyszczenia.")

    save_report(results, application_id(log))
    print("GOTOWE. Wyniki: results/analysis.md i results/analysis.json")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit("\nPrzerwano. Zadanie mogło dalej działać w YARN - sprawdź: http://localhost:8088")
    except (OSError, ValueError, RuntimeError) as error:
        sys.exit(f"BŁĄD: {error}")
