"""Wspólne funkcje skryptów, które uruchamiają polecenia i zadania Hadoop.

Polecenia wykonujemy w kontenerze resourcemanager. Katalogi scripts/ i data/ są w nim
zamontowane jako /project/scripts i /project/data (compose.yaml), więc kodu mappera
i reducera nie trzeba kopiować do kontenera.
"""
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "results"
STREAMING_JAR = "/opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.5.0.jar"


def hadoop(*command, stdin=None, check=True):
    """Wykonuje polecenie (hdfs, yarn, hadoop) w kontenerze resourcemanager."""
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", "resourcemanager", *command],
        cwd=ROOT, input=stdin, capture_output=True, text=True, encoding="utf-8")
    if check and result.returncode:
        raise RuntimeError(result.stderr.strip() or "Polecenie Docker zakończyło się błędem.")
    return result


def hdfs_exists(path):
    return hadoop("hdfs", "dfs", "-test", "-e", path, check=False).returncode == 0


def run_streaming(name, inputs, output, script, files=(), reducers=1, options=(), log_name="job.log"):
    """Uruchamia zadanie Hadoop Streaming i zwraca jego log.

    Mapper i reducer to ten sam skrypt Pythona, wywołany z argumentem map albo reduce.
    Ścieżki script i files podajemy względem katalogu projektu, np. "scripts/clean_flights.py".
    Pełny log trafia do results/<log_name>, a na ekranie widać tylko postęp.
    """
    command = ["docker", "compose", "exec", "-T", "resourcemanager", "hadoop", "jar", STREAMING_JAR,
               "-D", f"mapreduce.job.name={name}",
               "-D", f"mapreduce.job.reduces={reducers}"]
    for option in options:
        command += ["-D", option]
    command += ["-files", ",".join("/project/" + path for path in [script, *files])]
    for path in inputs:
        command += ["-input", path]
    script_name = Path(script).name
    command += ["-output", output,
                "-mapper", f"python3 {script_name} map",
                "-reducer", f"python3 {script_name} reduce",
                "-cmdenv", "PYTHONIOENCODING=utf-8",
                # Hadoop umieszcza każdy plik z -files w osobnym katalogu i daje do niego
                # dowiązanie w katalogu roboczym zadania. Bez tej zmiennej Python nie widzi
                # sąsiednich modułów (np. analyze_flights.py nie zaimportuje clean_flights.py).
                "-cmdenv", "PYTHONPATH=."]

    lines = []
    with subprocess.Popen(command, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                          text=True, encoding="utf-8") as process:
        for line in process.stdout:
            lines.append(line)
            if " map " in line or "ERROR" in line or "Submitted application" in line:
                print(line.rstrip())
    log = "".join(lines)
    RESULTS.mkdir(exist_ok=True)
    (RESULTS / log_name).write_text(log, encoding="utf-8")
    if process.returncode:
        raise RuntimeError(f"Zadanie Hadoop nie powiodło się. Szczegóły: results/{log_name}")
    return log


def application_id(log):
    """Identyfikator zadania YARN z logu, np. application_1793906622171_0001."""
    return re.search(r"application_\d+_\d+", log)[0]


def number(value):
    """12321982 -> '12 321 982'."""
    return f"{value:,}".replace(",", " ")
