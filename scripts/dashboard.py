"""Panel z wynikami analiz, czytający dane na bieżąco z MongoDB.

    streamlit run scripts/dashboard.py

Na pasku bocznym wybieramy bazę: lokalną albo Atlas. Obie zawierają te same wyniki;
próbka lotów jest tylko w bazie lokalnej.
"""
import io

import matplotlib.pyplot as plt
import streamlit as st

import charts
import queries
from common import number
from db import connect
from run_analysis import CAUSE_NAMES

st.set_page_config(page_title="Loty w USA – wyniki analiz", layout="wide")


@st.cache_resource
def database(target):
    # Jedno połączenie na bazę przez cały czas działania panelu.
    return connect(target)


CHART_WIDTH = 800  # px - wykres na cały wiersz nie rozciąga się na szerokość okna


def show(fig, place=st, width=CHART_WIDTH):
    """Wyświetla wykres jako obraz PNG o szerokości width (px albo "stretch").

    Obraz zawsze wypełnia pojemnik, a jego wielkość ustala sam pojemnik. Gdyby obrazowi
    podać szerokość w pikselach, Streamlit zmniejszyłby go na serwerze i wykres byłby nieostry.
    160 dpi daje ok. 1400 px szerokości - poniżej limitu, od którego Streamlit zmniejsza obrazy.
    """
    buffer = io.BytesIO()
    fig.savefig(buffer, format="png", dpi=160, bbox_inches="tight")
    plt.close(fig)  # matplotlib trzyma otwarte wykresy w pamięci
    place.container(width=width).image(buffer, width="stretch")


def percent(value, digits=1):
    return f"{value:.{digits}f}%".replace(".", ",")


def metric_choice(key):
    return st.radio("Miara", list(charts.METRICS), format_func=charts.METRICS.get, horizontal=True, key=key)


target = st.sidebar.radio("Baza danych", ["local", "atlas"],
                          format_func={"local": "lokalna (Docker)", "atlas": "MongoDB Atlas"}.get)
try:
    db = database(target)
except RuntimeError as error:
    st.error(str(error))
    st.stop()
last = queries.last_import(db)
st.sidebar.caption(f"Ostatni import: {last['started_utc']:%Y-%m-%d %H:%M} UTC" if last else "Brak importu.")

st.title("Opóźnienia i odwołania lotów w USA")
tabs = st.tabs(["Przegląd", "Przewoźnicy", "Sezonowość", "Pora dnia", "Przyczyny", "Lotniska", "Próbka lotów"])

with tabs[0]:
    summary = queries.total(db)
    cleaning = db["metadata"].find_one({"_id": "cleaning"})
    columns = st.columns(5)
    columns[0].metric("Loty", number(summary["flights"]))
    columns[1].metric("Na czas", percent(100 - summary["arr_delayed_pct"]))
    columns[2].metric("Średnie opóźnienie", f"{summary['arr_delay_mean']:.1f} min".replace(".", ","))
    columns[3].metric("Mediana opóźnienia", f"{summary['arr_delay_median']:.0f} min")
    columns[4].metric("Odwołane", percent(summary["cancelled_pct"], 2))
    st.write(f"Zakres danych: {cleaning['months'][0]} – {cleaning['months'][-1]}. "
             f"Wczytano {number(cleaning['counters']['records'])} rekordów, po czyszczeniu i usunięciu "
             f"duplikatów zostało {number(cleaning['counters']['written'])}. "
             "Lot jest na czas, gdy przyleciał z opóźnieniem mniejszym niż 15 minut.")
    st.caption("Źródło: BTS, Reporting Carrier On-Time Performance. Główny zbiór jest w HDFS, "
               "wyniki analiz MapReduce i dane ze scrapingu Wikipedii – w MongoDB.")

with tabs[1]:
    table = queries.carriers(db)
    metric = metric_choice("carriers")
    show(charts.carriers_chart(table, metric))
    st.dataframe(table[["key", "name", "type", "operates_as", "flights", "on_time_pct",
                        "arr_delay_mean", "arr_delay_median", "cancelled_pct"]],
                 hide_index=True, width="stretch")

with tabs[2]:
    metric = metric_choice("season")
    left, right = st.columns(2)
    show(charts.months_chart(queries.group_table(db, "month"), metric), left, "stretch")
    show(charts.weekdays_chart(queries.group_table(db, "weekday"), metric), right, "stretch")

with tabs[3]:
    show(charts.hours_chart(queries.group_table(db, "hour")))
    st.caption("Opóźnienia narastają w ciągu dnia: spóźniony samolot opóźnia kolejne loty.")

with tabs[4]:
    table = queries.carriers(db)
    choice = st.selectbox("Przewoźnik", ["wszyscy"] + list(table["label"]))
    if choice == "wszyscy":
        shares = queries.total(db)["cause_share_pct"]
    else:
        shares = table.loc[table["label"] == choice, "cause_share_pct"].iloc[0]
    show(charts.causes_chart(shares, f"Przyczyny opóźnień – {choice}"))
    st.caption("Przyczyny są raportowane tylko dla przylotów opóźnionych o co najmniej 15 minut.")

with tabs[5]:
    st.write("Wyniki lotnisk z MapReduce złączone (`$lookup`) z danymi ze scrapingu Wikipedii.")
    sizes = queries.airports_grouped(db, "role_group")
    left, right = st.columns(2)
    show(charts.sizes_chart(sizes), left, "stretch")
    right.dataframe(sizes, hide_index=True, width="stretch")
    states = queries.airports_grouped(db, "state")
    show(charts.states_chart(states))

with tabs[6]:
    if db["flights_sample"].estimated_document_count() == 0:
        st.info("Próbka lotów jest dostępna tylko w bazie lokalnej.")
    else:
        carriers = [""] + sorted(db["flights_sample"].distinct("Reporting_Airline"))
        left, middle, right = st.columns(3)
        carrier = left.selectbox("Przewoźnik", carriers, format_func=lambda code: code or "wszyscy")
        origin = middle.text_input("Lotnisko wylotu (kod, np. ATL)").strip().upper()
        cancelled = right.checkbox("Tylko odwołane")
        found, rows = queries.sample_flights(db, carrier or None, origin or None, cancelled)
        if found:
            st.write(f"Lotów: {number(found['flights'])}, odwołanych: {number(found['cancelled'])}, "
                     f"średnie opóźnienie przylotu: {found['arr_delay_mean'] or 0:.1f} min".replace(".", ","))
            st.dataframe(rows, hide_index=True, width="stretch")
        else:
            st.write("Brak lotów spełniających warunki.")
