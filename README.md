# Big Data — analiza lotów w USA

Projekt przetwarza dane o lotach krajowych w USA z wykorzystaniem HDFS i Hadoop MapReduce. Kod jest napisany w Pythonie, a klaster HDFS i YARN działa lokalnie w Dockerze.

## Dane

Źródło: **Reporting Carrier On-Time Performance (1987–present)**, Bureau of Transportation Statistics.

- [Opis zbioru](https://www.transtats.bts.gov/tables.asp?QO_VQ=EFD&QO_anzr=Nv4yv0r)
- [Opis kolumn](https://www.transtats.bts.gov/Fields.asp?gnoyr_VQ=FGJ)
- [Archiwa miesięczne ZIP](https://transtats.bts.gov/PREZIP/)

Pobrany zbiór obejmuje styczeń 2024 – wrzesień 2025: **21 plików CSV, 12 321 982 rekordy i 5,57 GB po rozpakowaniu**. Jeden rekord opisuje jeden lot. Pliki zawierają m.in. datę, przewoźnika, lotniska, opóźnienia oraz oznaczenia odwołania i przekierowania.

## Wymagania

Windows, Python 3.11 lub nowszy oraz uruchomiony Docker Desktop z Docker Compose. Skrypty korzystają wyłącznie z biblioteki standardowej Pythona.

Poniższe polecenia należy wykonywać w głównym folderze projektu.

## Pobieranie danych

```powershell
python scripts/download_flights.py
```

Skrypt pobiera miesięczne ZIP-y i rozpakowuje oryginalne CSV do `data/`. Sprawdza integralność archiwów, strukturę CSV i daty. W `data/manifest.json` zapisuje źródła, liczby rekordów, rozmiary i sumy SHA-256. Wykorzystane ZIP-y są usuwane.

Ponowne uruchomienie sprawdza i pomija gotowe miesiące. Po przerwaniu przez `Ctrl+C` wystarczy powtórzyć polecenie; niepełny transfer jest ponawiany od początku miesiąca.

## HDFS i YARN

Uruchomienie klastra:

```powershell
docker compose up -d
```

Panele: [HDFS — pliki i stan magazynu](http://localhost:9870), [YARN — zadania obliczeniowe](http://localhost:8088).

Podgląd zawartości plików w panelu HDFS korzysta z DataNode pod adresem `localhost:9864`. W lokalnej konfiguracji port jest udostępniony wyłącznie na tym komputerze, a DataNode zgłasza nazwę `localhost`, aby przekierowanie działało w przeglądarce. Woluminy przechowujące dane są niezależne od odtwarzania kontenerów.

Kopiowanie danych do HDFS:

```powershell
python scripts/import_hdfs.py
```

CSV trafiają do `/flights/raw/`, a manifest do `/flights/manifest.json`. Import sprawdza SHA-256 lokalnego pliku i kopii w HDFS. Ponowne uruchomienie pomija zgodne kopie; pliku o innej zawartości pod tą samą nazwą nie nadpisuje.

Oryginały pozostają w `data/`. HDFS działa lokalnie w woluminach Dockera — obie kopie CSV zajmują łącznie około **11,15 GB**, poza miejscem na obrazy Dockera i pliki robocze.

## Sprawdzanie i czyszczenie danych w Hadoop

```powershell
python scripts/run_cleaning.py                  # wszystkie miesiące -> /flights/clean
python scripts/run_cleaning.py --month 2024-01  # jeden miesiąc -> /flights/clean-2024-01
```

Wymagane są działające kontenery i dane w HDFS. `run_cleaning.py` uruchamia zadanie Hadoop Streaming w YARN. Plik `clean_flights.py` zawiera obie części zadania:

- **mapper** sprawdza i czyści każdy rekord, a poprawny wypisuje z kluczem lotu (data, przewoźnik, numer lotu, trasa, planowana godzina odlotu);
- **reducer** dostaje rekordy posortowane po tym kluczu, zapisuje pierwszy rekord z każdej grupy, a pozostałe liczy jako duplikaty.

Katalogi `scripts/` i `data/` są zamontowane w kontenerze `resourcemanager` tylko do odczytu, więc po zmianie `compose.yaml` trzeba raz wykonać `docker compose up -d`.

Reguły:

- **Odrzucenie rekordu**: błędna struktura CSV, data spoza miesiąca pliku, brak przewoźnika, błędny kod (3 litery) lub ID lotniska, status odwołania/przekierowania inny niż 0/1 albo oba naraz, lot odbyty (nie odwołany i nie przekierowany) bez opóźnienia przylotu.
- **Ujednolicenie**: usunięcie spacji na brzegach pól, kody lotnisk wielkimi literami, statusy `1.00` → `1`, godzina odlotu w formacie `HHMM` (dopuszczalne `2400`).
- **Błędne wartości opcjonalne** (np. tekst zamiast liczby, ujemny dystans) zamieniamy na brak, nie na zero.
- **Nierealny czas lotu**: jeśli z `Distance` i `AirTime` wychodzi średnia prędkość poza zakresem 100–700 mph, `AirTime` zamieniamy na brak.
- **Duplikaty**: z rekordów o tym samym kluczu lotu zostaje pierwszy.
- **Skrajne opóźnienia** (przylot ponad 24 godziny po czasie) zostawiamy, bo mogą być prawdziwe, ale liczymy je w raporcie.
- **Zostawiamy**: ujemne opóźnienia (lot przed czasem), loty odwołane i przekierowane oraz braki danych. Braki przyczyn opóźnień nie są błędem: źródło wypełnia je tylko dla przylotów opóźnionych o co najmniej 15 minut ([zasady raportowania](https://www.bts.gov/explore-topics-and-geography/modes/aviation/number-39-technical-directive-reporting-time)).

Wynik w HDFS to CSV z 21 wybranymi kolumnami, bez nagłówka; kolejność kolumn opisuje `_schema.json` w katalogu wyniku. Oryginalne pliki w `/flights/raw/` pozostają bez zmian. Istniejący katalog wyniku nie jest nadpisywany — skrypt podaje polecenie, którym można go usunąć.

Po zakończeniu skrypt sprawdza, czy liczba rekordów każdego miesiąca zgadza się z manifestem i czy każdy rekord został zapisany, odrzucony albo uznany za duplikat. Wyniki:

- `results/data_quality.md` — raport do czytania,
- `results/data_quality.json` — te same liczniki w formacie JSON,
- `results/cleaning.log` — pełny log Hadoop ostatniego przebiegu.

## Testy

```powershell
python -B -m unittest discover -s tests -v
```

Testy obejmują pobieranie danych, import do HDFS i reguły czyszczenia, w tym CSV z przecinkami wewnątrz pól, fragmenty bez nagłówka, braki, błędne statusy oraz wartości liczbowe. Testy pobierania i importu korzystają z blokady plików Windows (`msvcrt`), więc uruchamia się je na Windows.
