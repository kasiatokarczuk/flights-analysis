"""Uruchamia czyszczenie danych w Hadoop (YARN) i zapisuje raport jakości.

    python scripts/run_cleaning.py                   # wszystkie miesiące -> /flights/clean
    python scripts/run_cleaning.py --month 2024-01   # jeden miesiąc -> /flights/clean-2024-01

Kod mappera i reducera oraz manifest są widoczne w kontenerze resourcemanager dzięki
zamontowanym katalogom (compose.yaml), więc nie trzeba ich kopiować.
"""
import argparse
import json
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from clean_flights import COLUMNS, NUMBERS

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "results"
STREAMING_JAR = "/opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.5.0.jar"
REASONS = {"structure": "błędna struktura CSV", "date": "data spoza miesiąca pliku",
           "carrier": "brak przewoźnika", "airport": "błędny kod lub ID lotniska",
           "status": "błędny status odwołania/przekierowania",
           "no_arrival": "lot odbyty, ale bez opóźnienia przylotu"}


def hadoop(*command, stdin=None, check=True):
    """Wykonuje polecenie (hdfs, yarn, hadoop) w kontenerze resourcemanager."""
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", "resourcemanager", *command],
        cwd=ROOT, input=stdin, capture_output=True, text=True, encoding="utf-8")
    if check and result.returncode:
        raise RuntimeError(result.stderr.strip() or "Polecenie Docker zakończyło się błędem.")
    return result


def run_job(months, output):
    """Uruchamia zadanie Hadoop Streaming (mapper czyści, reducer usuwa duplikaty) i zwraca log."""
    command = ["docker", "compose", "exec", "-T", "resourcemanager", "hadoop", "jar", STREAMING_JAR,
               "-D", "mapreduce.job.name=flights-cleaning",
               "-D", "mapreduce.job.reduces=2",
               "-D", "mapreduce.job.counters.max=256",
               # Reducer wypisuje gotową linię CSV. Hadoop dzieli ją na klucz i wartość
               # po pierwszym przecinku i skleja z powrotem tym samym przecinkiem.
               "-D", "stream.reduce.output.field.separator=,",
               "-D", "mapreduce.output.textoutputformat.separator=,",
               "-files", "/project/scripts/clean_flights.py,/project/data/manifest.json"]
    for month in months:
        command += ["-input", f"/flights/raw/{month}.csv"]
    command += ["-output", output,
                "-mapper", "python3 clean_flights.py map",
                "-reducer", "python3 clean_flights.py reduce",
                "-cmdenv", "PYTHONIOENCODING=utf-8"]

    # Pełny log zapisujemy do pliku, na ekranie pokazujemy tylko postęp.
    lines = []
    with subprocess.Popen(command, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                          text=True, encoding="utf-8") as process:
        for line in process.stdout:
            lines.append(line)
            if " map " in line or "ERROR" in line or "Submitted application" in line:
                print(line.rstrip())
    log = "".join(lines)
    (RESULTS / "cleaning.log").write_text(log, encoding="utf-8")
    if process.returncode:
        raise RuntimeError("Zadanie Hadoop nie powiodło się. Szczegóły: results/cleaning.log")
    return log


def read_counters(log):
    """Odczytuje z logu Hadoop liczniki z grupy FlightQuality."""
    counts, inside = {}, False
    for line in log.splitlines():
        if line.strip() == "FlightQuality":
            inside = True
        elif inside:
            match = re.fullmatch(r"\s+([\w-]+)=(\d+)", line)
            if not match:
                break
            counts[match[1]] = int(match[2])
    return counts


def check_counts(counts, manifest, months):
    """Sprawdza, czy każdy rekord z manifestu został zapisany, odrzucony albo uznany za duplikat."""
    for month in months:
        if counts.get("month_" + month) != manifest["files"][month]["records"]:
            raise ValueError(f"Liczba rekordów {month} nie zgadza się z manifestem.")
    if counts.get("headers") != len(months):
        raise ValueError("Liczba nagłówków nie zgadza się z liczbą plików.")
    rejected = sum(v for k, v in counts.items() if k.startswith("rejected_"))
    if counts.get("valid", 0) + rejected != counts["records"]:
        raise ValueError("Rekordy poprawne i odrzucone nie sumują się do wejścia.")
    if counts.get("written", 0) + counts.get("duplicates", 0) != counts.get("valid", 0):
        raise ValueError("Rekordy zapisane i duplikaty nie sumują się do poprawnych.")


def number(value):
    return f"{value:,}".replace(",", " ")


def save_report(counts, months, output, application):
    """Zapisuje raport w dwóch wersjach: JSON i krótki Markdown."""
    report = {"finished_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
              "application_id": application, "hdfs_output": output,
              "months": months, "counters": counts}
    (RESULTS / "data_quality.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

    get = lambda name: counts.get(name, 0)
    rejected = {reason: get("rejected_" + reason) for reason in REASONS}
    lines = [
        "# Raport jakości danych", "",
        f"Zakres: {months[0]} – {months[-1]} ({len(months)} plików). "
        f"Zadanie YARN: `{application}`. Wynik: `{output}` w HDFS.",
        "Reguły czyszczenia opisano w README (sekcja o czyszczeniu danych).", "",
        "## Rekordy", "",
        "| | Liczba |", "|---|---:|",
        f"| Wczytane | {number(get('records'))} |",
        f"| Odrzucone | {number(sum(rejected.values()))} |",
        f"| Duplikaty | {number(get('duplicates'))} |",
        f"| Zapisane | {number(get('written'))} |",
        f"| – w tym loty odwołane | {number(get('cancelled'))} |",
        f"| – w tym loty przekierowane | {number(get('diverted'))} |", "",
    ]
    if any(rejected.values()):
        lines += ["## Powody odrzucenia", "", "| Powód | Liczba |", "|---|---:|"]
        lines += [f"| {REASONS[r]} | {number(n)} |" for r, n in rejected.items() if n]
        lines.append("")
    lines += ["## Braki i błędne wartości", "",
              "Błędną wartość zastępujemy brakiem (pustym polem), nie zerem.", "",
              "| Pole | Braki w źródle | Błędne → brak |", "|---|---:|---:|"]
    for name in ["CRSDepTime"] + NUMBERS:
        lines.append(f"| {name} | {number(get('missing_' + name))} | {number(get('invalid_' + name))} |")
    lines += ["", "## Wartości podejrzane", "",
              "| Kontrola | Liczba | Co robimy |", "|---|---:|---|",
              f"| `AirTime` daje prędkość poza zakresem 100–700 mph | "
              f"{number(get('implausible_AirTime'))} | zamiana na brak |",
              f"| `ArrDelay` powyżej 24 godzin | {number(get('extreme_ArrDelay'))} | "
              "zostawiamy, tylko liczymy |"]
    lines += ["",
              "Braki `ArrDelay` i `AirTime` dotyczą głównie lotów odwołanych i przekierowanych. "
              "Przyczyny opóźnień (`CarrierDelay` … `LateAircraftDelay`) są w źródle wypełniane "
              "tylko dla przylotów opóźnionych o co najmniej 15 minut, więc ich brak nie jest błędem.", ""]
    (RESULTS / "data_quality.md").write_text("\n".join(lines), encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description="Czyszczenie danych w Hadoop.")
    parser.add_argument("--month", help="np. 2024-01; domyślnie wszystkie miesiące")
    args = parser.parse_args()
    manifest = json.loads((ROOT / "data" / "manifest.json").read_text(encoding="utf-8"))
    if args.month and args.month not in manifest["files"]:
        parser.error("Tego miesiąca nie ma w manifeście.")
    months = [args.month] if args.month else sorted(manifest["files"])
    output = "/flights/clean" + (f"-{args.month}" if args.month else "")

    # Istniejącego wyniku nie nadpisujemy - trzeba go usunąć ręcznie.
    if hadoop("hdfs", "dfs", "-test", "-e", output, check=False).returncode == 0:
        sys.exit(f"Katalog {output} już istnieje w HDFS. Aby powtórzyć czyszczenie, usuń go:\n"
                 f"  docker compose exec resourcemanager hdfs dfs -rm -r {output}")

    RESULTS.mkdir(exist_ok=True)
    print(f"Czyszczenie {len(months)} plików w Hadoop. Wynik: {output}")
    log = run_job(months, output)
    counts = read_counters(log)
    check_counts(counts, manifest, months)
    application = re.search(r"application_\d+_\d+", log)[0]

    # Pliki part-* nie mają nagłówka; kolejność kolumn zapisujemy obok wyniku.
    schema = json.dumps({"format": "csv", "header": False, "columns": COLUMNS}, indent=2)
    hadoop("hdfs", "dfs", "-put", "-", output + "/_schema.json", stdin=schema)
    save_report(counts, months, output, application)
    print(f"GOTOWE: zapisano {number(counts['written'])} rekordów. Raport: results/data_quality.md")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit("\nPrzerwano. Zadanie mogło dalej działać w YARN - sprawdź: http://localhost:8088")
    except (OSError, ValueError, RuntimeError) as error:
        sys.exit(f"BŁĄD: {error}")
