"""Wykresy wyników analiz. Te same funkcje rysują wykresy w panelu (dashboard.py)
i zapisują je jako PNG do sprawozdania:

    python scripts/charts.py                 # dane z lokalnej bazy
    python scripts/charts.py --target atlas  # dane z Atlas

Pliki PNG trafiają do results/charts/.
"""
import argparse
import sys

import matplotlib
import matplotlib.pyplot as plt
from pymongo.errors import PyMongoError

import queries
from common import RESULTS
from db import TARGETS, connect
from run_analysis import CAUSE_NAMES, WEEKDAYS

COLOR = "#2f6b9a"
METRICS = {
    "on_time_pct": "Przyloty na czas [%]",
    "arr_delay_mean": "Średnie opóźnienie przylotu [min]",
    "arr_delay_median": "Mediana opóźnienia przylotu [min]",
    "cancelled_pct": "Odwołane loty [%]",
}


def bar_chart(labels, values, title, value_label, horizontal=False):
    """Prosty wykres słupkowy. Poziomy, gdy etykiety są długie (np. nazwy przewoźników)."""
    fig, ax = plt.subplots(figsize=(9, 5))
    if horizontal:
        ax.barh(labels, values, color=COLOR)
        ax.invert_yaxis()  # pierwszy element na górze
        ax.set_xlabel(value_label)
        ax.grid(axis="x", alpha=0.3)
    else:
        ax.bar(labels, values, color=COLOR)
        ax.set_ylabel(value_label)
        ax.grid(axis="y", alpha=0.3)
        ax.tick_params(axis="x", rotation=45 if len(labels) > 12 else 0)
    ax.set_axisbelow(True)
    ax.set_title(title)
    fig.tight_layout()
    return fig


def carriers_chart(table, metric="on_time_pct"):
    table = table.sort_values(metric, ascending=metric != "on_time_pct")
    return bar_chart(table["label"], table[metric], "Przewoźnicy", METRICS[metric], horizontal=True)


def months_chart(table, metric="on_time_pct"):
    return bar_chart(table["key"], table[metric], "Miesiące", METRICS[metric])


def weekdays_chart(table, metric="on_time_pct"):
    return bar_chart([WEEKDAYS[day] for day in table["key"]], table[metric], "Dni tygodnia", METRICS[metric])


def hours_chart(table):
    return bar_chart([f"{hour}:00" for hour in table["key"]], table["dep_delayed_pct"],
                     "Godzina planowanego odlotu", "Opóźnione odloty (≥ 15 min) [%]")


def causes_chart(shares, title="Przyczyny opóźnień"):
    labels = [CAUSE_NAMES[cause] for cause in shares]
    return bar_chart(labels, list(shares.values()), title, "Udział w minutach opóźnień [%]", horizontal=True)


def sizes_chart(table):
    return bar_chart(table["name"], table["dep_delayed_pct"], "Wielkość lotniska (kategoria FAA)",
                     "Opóźnione odloty (≥ 15 min) [%]")


def states_chart(table, top=15):
    table = table.head(top).sort_values("dep_delayed_pct", ascending=False)
    return bar_chart(table["name"], table["dep_delayed_pct"],
                     f"Stany z największą liczbą odlotów (top {top})", "Opóźnione odloty (≥ 15 min) [%]",
                     horizontal=True)


def all_charts(database):
    """Wszystkie wykresy do sprawozdania: {nazwa pliku: wykres}."""
    return {
        "przewoznicy.png": carriers_chart(queries.carriers(database)),
        "miesiace.png": months_chart(queries.group_table(database, "month")),
        "dni_tygodnia.png": weekdays_chart(queries.group_table(database, "weekday")),
        "godziny.png": hours_chart(queries.group_table(database, "hour")),
        "przyczyny.png": causes_chart(queries.total(database)["cause_share_pct"]),
        "wielkosc_lotnisk.png": sizes_chart(queries.airports_grouped(database, "role_group")),
        "stany.png": states_chart(queries.airports_grouped(database, "state")),
    }


def main():
    parser = argparse.ArgumentParser(description="Wykresy PNG z wyników w MongoDB.")
    parser.add_argument("--target", choices=TARGETS, default="local")
    args = parser.parse_args()
    matplotlib.use("Agg")  # bez okien, tylko zapis do plików
    folder = RESULTS / "charts"
    folder.mkdir(exist_ok=True)
    for name, fig in all_charts(connect(args.target)).items():
        fig.savefig(folder / name, dpi=150)
        plt.close(fig)
        print(f"zapisano results/charts/{name}")


if __name__ == "__main__":
    try:
        main()
    except (OSError, RuntimeError, PyMongoError) as error:
        sys.exit(f"BŁĄD: {error}")
