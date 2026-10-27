"""Uruchom czyszczenie w Hadoop/YARN. Opcja --month wybiera jeden miesiąc."""

import argparse
import hashlib
import json
import re
import shutil
import subprocess
import sys
from datetime import datetime, timezone

from clean_flights import COLUMNS
from download_flights import OUTPUT, directory_lock
from import_hdfs import docker


ROOT = OUTPUT.parent
RESULTS = ROOT / "results"


def compose(*args, **kwargs):
    return subprocess.run(["docker", "compose", "-f", str(ROOT / "compose.yaml"), *args],
                          cwd=ROOT, check=True, **kwargs)


def read_counters(log):
    """Odczytaj z logu tylko grupę liczników FlightQuality."""
    counts, inside = {}, False
    for line in log.splitlines():
        if line.strip() == "FlightQuality":
            inside = True
        elif inside:
            match = re.fullmatch(r"\s+([A-Za-z0-9_]+)=(\d+)", line)
            if match:
                counts[match[1]] = int(match[2])
            else:
                inside = False
    return counts


def save_report(report):
    """Zapisz liczniki Hadoop jako raport do czytania oraz JSON."""
    counts = report["counters"]
    labels = {
        "records": "Rekordy wejściowe", "written": "Zapisane rekordy",
        "headers": "Pominięte nagłówki", "trimmed_rows": "Rekordy z usuniętymi spacjami",
        "status_normalized_rows": "Rekordy ze statusem ujednoliconym do 0/1",
        "airport_case_fixed": "Kody lotnisk zmienione na wielkie litery",
        "time_normalized_rows": "Godziny uzupełnione do HHMM",
        "rejected_structure": "Odrzucone: struktura CSV",
        "rejected_date": "Odrzucone: data lub miesiąc",
        "rejected_carrier": "Odrzucone: brak przewoźnika",
        "rejected_airport": "Odrzucone: kod lotniska",
        "rejected_airport_id": "Odrzucone: identyfikator lotniska",
        "rejected_status": "Odrzucone: brak, błąd lub konflikt statusów",
        "regular": "Nieodwołane i nieprzekierowane",
        "cancelled": "Odwołane", "diverted": "Przekierowane",
        "missing_arrival_regular": "Brak ArrDelay: nieodwołane i nieprzekierowane",
        "missing_arrival_cancelled": "Brak ArrDelay: odwołane",
        "missing_arrival_diverted": "Brak ArrDelay: przekierowane",
    }
    text = f"""# Sprawdzanie i czyszczenie danych

Zakres: {', '.join(report['months'])}.

Zadanie YARN: `{report['application_id']}`, **FINISHED / SUCCEEDED**.
Dane oczyszczone: `{report['hdfs_output']}`. Oryginały pozostają w `/flights/raw/`.

## Reguły

- Odrzucamy rekordy z błędną strukturą, datą spoza miesiąca pliku, brakiem przewoźnika, błędnym lotniskiem/ID lub statusem. Każde odrzucenie ma jeden, pierwszy wykryty powód.
- Usuwamy otaczające spacje; kody lotnisk zapisujemy wielkimi literami, a statusy jako 0/1.
- Poprawne godziny planowanego odlotu uzupełniamy do HHMM; zachowujemy dopuszczalne 2400.
- Błędne opcjonalne liczby i godziny zastępujemy pustym polem, nie zerem. Liczymy je osobno od pierwotnych braków.
- Zachowujemy ujemne opóźnienia, odwołania, przekierowania oraz braki danych. Brak przyczyny opóźnienia nie jest automatycznie zerem.
- Zachowujemy 21 wybranych kolumn. Redukcja kolumn nie jest sama w sobie czyszczeniem.

## Kontrola i zmiany

| Kontrola | Liczba |
|---|---:|
"""
    for name, label in labels.items():
        text += f"| {label} | {counts.get(name, 0)} |\n"
    text += "\nStatusy i braki pól liczbowych dotyczą zachowanych rekordów.\n"
    text += "\n## Pola opcjonalne\n\n| Pole | Pierwotne braki | Błędne wartości zamienione na brak |\n|---|---:|---:|\n"
    for name in ["CRSDepTime", "DepDelay", "ArrDelay", "Distance", "AirTime",
                 "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay"]:
        text += f"| {name} | {counts.get('missing_' + name, 0)} | {counts.get('invalid_' + name, 0)} |\n"
    text += "\nBrak przyczyny opóźnienia sam w sobie nie oznacza błędu. Źródło nie wymaga tych danych dla przylotów z opóźnieniem poniżej 15 minut ani dla lotów przekierowanych. Przyczyny odwołań są raportowane osobno.\n"
    text += "\n## Rekordy według pliku źródłowego\n\n| Miesiąc | Odczytane rekordy |\n|---|---:|\n"
    for month in report["months"]:
        text += f"| {month} | {counts['month_' + month.replace('-', '_')]} |\n"
    text += "\nWszystkie liczebności miesięczne i liczba nagłówków są zgodne z manifestem. Zapisane i odrzucone rekordy rozliczają całe wejście.\n"
    text += "\n## Schemat wyjścia i zakres kontroli\n\n" + ", ".join(COLUMNS) + "\n"
    text += "\nPliki `part-*` są CSV bez nagłówka; kolejność kolumn opisuje `_schema.json` w katalogu wyjściowym. `_quality.json` zawiera liczniki i metadane.\n"
    text += "\nTo podstawowe sprawdzanie wybranych pól. Nie wykonano globalnego usuwania duplikatów ani kontroli pozostałych kolumn. Braków nie imputowano. Ten etap nie wykonuje analiz statystycznych.\n"
    text += "\nKod, manifest i aplikację identyfikują wartości w `data_quality.json`. Przebieg: `cleaning.log`.\n"
    text += "\n[Zasady raportowania przyczyn opóźnień i odwołań](https://www.bts.gov/explore-topics-and-geography/modes/aviation/number-39-technical-directive-reporting-time).\n"
    (RESULTS / "data_quality.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    (RESULTS / "data_quality.md").write_text(text, encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--month", help="Np. 2024-01. Domyslnie wszystkie miesiace.")
    args = parser.parse_args()
    manifest = json.loads((OUTPUT / "manifest.json").read_text(encoding="utf-8"))
    if args.month and args.month not in manifest["files"]:
        parser.error("Miesiac nie wystepuje w manifescie.")
    months = [args.month] if args.month else sorted(manifest["files"])
    output = "/flights/clean" + ("-" + args.month if args.month else "")
    # Zapisujemy nowy wynik, bez automatycznego kasowania istniejących danych.
    if docker("hdfs", "dfs", "-test", "-e", output, check=False).returncode == 0:
        raise RuntimeError(
            f"Katalog {output} juz istnieje. Zachowano jego zawartosc.\n"
            "Jesli to niepotrzebny wynik nieudanego przebiegu, sprawdz w YARN, czy zadanie juz nie dziala.\n"
            "Dopiero wtedy mozesz usunac ten katalog i powtorzyc uruchomienie:\n"
            f"docker compose exec -T namenode hdfs dfs -rm -r {output}"
        )
    if json.loads(docker("hdfs", "dfs", "-cat", "/flights/manifest.json").stdout) != manifest:
        raise ValueError("Manifest HDFS rozni sie od lokalnego.")
    # Rozmiar wejścia stanowi zapas na wynik; dodatkowe 2 GB zostawiamy na pracę HDFS.
    required_space = sum(manifest["files"][month]["bytes"] for month in months) + 2_000_000_000
    if shutil.disk_usage(ROOT).free < required_space:
        raise RuntimeError(f"Wymagane co najmniej {required_space / 1_000_000_000:.2f} GB wolnego miejsca na dysku projektu.")

    stage = "/tmp/flights-cleaning"
    # Do kontenera trafiają kod i manifest, bez dodatkowej kopii surowych CSV.
    compose("exec", "-T", "resourcemanager", "mkdir", "-p", stage)
    for source in (ROOT / "scripts/clean_flights.py", OUTPUT / "manifest.json"):
        compose("cp", str(source), "resourcemanager:" + stage + "/" + source.name,
                capture_output=True)
    command = ["docker", "compose", "-f", str(ROOT / "compose.yaml"), "exec", "-T", "resourcemanager",
               "hadoop", "jar", "/opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.5.0.jar"]
    # Każdy rekord czyścimy niezależnie; liczniki sumuje Hadoop, więc reducer nie jest potrzebny.
    for option in ("mapreduce.job.name=flights-check-and-clean", "mapreduce.job.reduces=0",
                   "mapreduce.map.speculative=false", "mapreduce.job.counters.max=256",
                   "mapreduce.input.fileinputformat.split.minsize=268435456",
                   "stream.map.output.field.separator=,", "mapreduce.output.textoutputformat.separator=,"):
        command += ["-D", option]
    command += ["-files", stage + "/clean_flights.py," + stage + "/manifest.json"]
    for month in months:
        command += ["-input", "/flights/raw/" + month + ".csv"]
    command += ["-output", output, "-mapper", "python3 clean_flights.py",
                "-cmdenv", "PYTHONIOENCODING=utf-8"]
    RESULTS.mkdir(exist_ok=True)
    print(f"Hadoop: sprawdzanie i czyszczenie {len(months)} miesiecy -> {output}", flush=True)
    application = None
    try:
        # Postęp pokazujemy w terminalu, a pełne komunikaty zapisujemy w logu.
        with (RESULTS / "cleaning.log").open("a", encoding="utf-8") as logfile:
            process = subprocess.Popen(command, cwd=ROOT, stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT, text=True, encoding="utf-8")
            lines = []
            for line in process.stdout:
                lines.append(line)
                logfile.write(line)
                logfile.flush()
                match = re.search(r"application_\d+_\d+", line)
                if match:
                    application = match.group()
                if any(word in line for word in ("application_", "map ", "ERROR", "failed")):
                    print(line.rstrip(), flush=True)
            if process.wait():
                raise RuntimeError("Zadanie nie powiodlo sie. Szczegoly: results/cleaning.log.")
        log = "".join(lines)
        if not application:
            raise RuntimeError("Nie odczytano identyfikatora aplikacji YARN.")
        # Przed raportem potwierdzamy zakończenie zadania i rozliczamy wszystkie rekordy.
        state = json.loads(docker("curl", "--fail", "-s", "http://resourcemanager:8088/ws/v1/cluster/apps/" + application).stdout)["app"]
        if state["state"] != "FINISHED" or state["finalStatus"] != "SUCCEEDED":
            raise RuntimeError("YARN nie potwierdzil SUCCEEDED.")
        docker("hdfs", "dfs", "-test", "-e", output + "/_SUCCESS")
        counts = read_counters(log)
        expected = sum(manifest["files"][month]["records"] for month in months)
        if counts.get("records") != expected or counts.get("headers") != len(months):
            raise ValueError("Liczba rekordow lub naglowkow nie zgadza sie z manifestem.")
        for month in months:
            if counts.get("month_" + month.replace("-", "_")) != manifest["files"][month]["records"]:
                raise ValueError("Liczba rekordow miesiaca nie zgadza sie z manifestem: " + month)
        rejected = sum(value for name, value in counts.items() if name.startswith("rejected_"))
        if counts.get("written", 0) + rejected != expected:
            raise ValueError("Rekordy zachowane i odrzucone nie sumuja sie do wejscia.")
        if sum(counts.get(name, 0) for name in ("regular", "cancelled", "diverted")) != counts.get("written", 0):
            raise ValueError("Statusy lotow nie sumuja sie do liczby zapisanych rekordow.")
        report = {"application_id": application, "months": months, "hdfs_output": output,
                  "finished_utc": datetime.now(timezone.utc).isoformat(), "counters": counts,
                  "worker_sha256": hashlib.sha256((ROOT / "scripts/clean_flights.py").read_bytes()).hexdigest(),
                  "manifest_sha256": hashlib.sha256((OUTPUT / "manifest.json").read_bytes()).hexdigest()}
        save_report(report)
        # Pliki zaczynające się od _ nie są wejściem kolejnych zadań Hadoop.
        schema = RESULTS / "_schema.json"
        schema.write_text(json.dumps({"format": "csv", "header": False, "columns": COLUMNS}, indent=2) + "\n", encoding="utf-8")
        compose("cp", str(schema), "resourcemanager:" + stage + "/_schema.json", capture_output=True)
        compose("cp", str(RESULTS / "data_quality.json"), "resourcemanager:" + stage + "/_quality.json", capture_output=True)
        compose("exec", "-T", "resourcemanager", "hdfs", "dfs", "-put", stage + "/_schema.json", stage + "/_quality.json", output)
        schema.unlink()
        print(f"GOTOWE: {counts.get('written', 0)} zapisanych, {rejected} odrzuconych. Raport: results/data_quality.md.")
    except KeyboardInterrupt:
        # Ctrl+C zatrzymuje również zadanie YARN, jeśli znamy jego identyfikator.
        if application:
            compose("exec", "-T", "resourcemanager", "yarn", "application", "-kill", application)
        raise
    finally:
        # Błąd sprzątania nie może zastąpić informacji o wyniku zadania.
        try:
            compose("exec", "-T", "resourcemanager", "rm", "-f", stage + "/clean_flights.py", stage + "/manifest.json",
                    stage + "/_schema.json", stage + "/_quality.json", capture_output=True)
            compose("exec", "-T", "resourcemanager", "rmdir", stage, capture_output=True)
        except (OSError, subprocess.CalledProcessError) as error:
            print(f"Nie udalo sie posprzatac plikow tymczasowych w {stage}: {error}", file=sys.stderr)


if __name__ == "__main__":
    try:
        with directory_lock(OUTPUT):
            main()
    except KeyboardInterrupt:
        print("Przerwano. Sprawdz stan zadania i katalog wyjsciowy w HDFS.", file=sys.stderr)
        sys.exit(130)
    except (OSError, ValueError, RuntimeError, subprocess.CalledProcessError) as error:
        print("BLAD:", error, file=sys.stderr)
        sys.exit(1)
