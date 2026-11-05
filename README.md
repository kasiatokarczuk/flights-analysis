# Big Data — analiza lotów w USA

Projekt przetwarza dane o lotach krajowych w USA z wykorzystaniem HDFS i Hadoop MapReduce. Kod jest napisany w Pythonie, a klaster HDFS i YARN działa lokalnie w Dockerze.

## Dane

Źródło: **Reporting Carrier On-Time Performance (1987–present)**, Bureau of Transportation Statistics.

- [Opis zbioru](https://www.transtats.bts.gov/tables.asp?QO_VQ=EFD&QO_anzr=Nv4yv0r)
- [Opis kolumn](https://www.transtats.bts.gov/Fields.asp?gnoyr_VQ=FGJ)
- [Archiwa miesięczne ZIP](https://transtats.bts.gov/PREZIP/)

Pobrany zbiór obejmuje styczeń 2024 – wrzesień 2025: **21 plików CSV, 12 321 982 rekordy i 5,57 GB po rozpakowaniu**. Jeden rekord opisuje jeden lot. Pliki zawierają m.in. datę, przewoźnika, lotniska, opóźnienia oraz oznaczenia odwołania i przekierowania.

## Wymagania

Windows, Python 3.11 lub nowszy oraz uruchomiony Docker Desktop z Docker Compose. Biblioteki potrzebne do scrapingu:

```powershell
pip install -r requirements.txt
```

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

Panele:

- [HDFS — pliki i stan magazynu](http://localhost:9870) (menu *Utilities → Browse the file system*),
- [YARN — zadania obliczeniowe](http://localhost:8088) (tylko zadania od ostatniego startu kontenera),
- [Historia zadań MapReduce](http://localhost:19888) (zakończone zadania z licznikami i czasami, także po restarcie).

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

## Analizy statystyczne w Hadoop

```powershell
python scripts/run_analysis.py
```

Wejściem są oczyszczone dane z `/flights/clean`, a wynik trafia do `/flights/analysis` w HDFS. Mapper i reducer są w `analyze_flights.py`. Jedno zadanie MapReduce liczy statystyki dla pięciu analiz:

1. **Punktualność przewoźników** — liczba lotów, odsetek przylotów na czas, średnie i mediana opóźnienia przylotu, odsetek odwołań.
2. **Sezonowość** — te same miary według miesięcy i dni tygodnia.
3. **Godzina planowanego odlotu** — odsetek opóźnionych odlotów i przylotów oraz średnie opóźnienie odlotu.
4. **Przyczyny opóźnień** — udział minut opóźnienia z pięciu przyczyn raportowanych przez BTS, łącznie i dla każdego przewoźnika.
5. **Lotniska** — liczba odlotów, odsetek opóźnionych odlotów, średnie opóźnienie odlotu i odsetek odwołań.

Lot jest „na czas”, gdy przyleciał z opóźnieniem mniejszym niż 15 minut (definicja BTS).

Mapper nie wypisuje każdego lotu osobno. Dla każdej grupy (przewoźnik, miesiąc, dzień tygodnia, godzina, lotnisko, cały zbiór) sumuje statystyki w pamięci i wypisuje je na końcu (*in-mapper combining*). Dzięki temu do reducera trafia kilkaset linii zamiast 12 mln. Reducer łączy sumy z wszystkich mapperów i liczy wskaźniki. Mediana jest dokładna, bo mapper zapisuje histogram opóźnień co do minuty, a histogramy z różnych mapperów można po prostu zsumować.

Po zakończeniu skrypt sprawdza, czy liczba lotów w analizie zgadza się z liczbą rekordów zapisanych przy czyszczeniu. Wyniki:

- `results/analysis.md` — tabele wszystkich analiz,
- `results/analysis.json` — pełne wyniki (m.in. wszystkie lotniska),
- `results/analysis.log` — pełny log Hadoop.

Wspólne funkcje obu skryptów uruchamiających zadania (`run_cleaning.py`, `run_analysis.py`) są w `common.py`.

## Web scraping: lotniska i przewoźnicy z Wikipedii

```powershell
python scripts/scrape_wikipedia.py            # użyj zapisanych stron, jeśli są
python scripts/scrape_wikipedia.py --refresh  # pobierz strony od nowa
```

Skrypt pobiera dwie strony Wikipedii (`requests`) i odczytuje z nich tabele (`BeautifulSoup`):

- [List of airports in the United States](https://en.wikipedia.org/wiki/List_of_airports_in_the_United_States) — kody FAA/IATA/ICAO, nazwa, miasto, stan, kategoria FAA (*Role*) i liczba pasażerów odprawionych w 2019 roku. Stan nie jest osobną kolumną: wiersz z samą nazwą stanu (np. „ALABAMA”) poprzedza lotniska z tego stanu.
- [List of airlines of the United States](https://en.wikipedia.org/wiki/List_of_airlines_of_the_United_States) — nazwa, kody IATA/ICAO, typ (linia główna, regionalna, cargo… według sekcji strony) i informacja, pod jaką marką lata linia regionalna (np. *Operates as Delta Connection*).

Kategoria FAA opisuje wielkość lotniska według udziału w ruchu pasażerskim USA: P-L (duże, co najmniej 1%), P-M (średnie, 0,25–1%), P-S (małe, 0,05–0,25%), P-N (bardzo małe, poniżej 0,05%). Pozostałe oznaczenia (CS, R, GA) i brak kategorii trafiają do grupy „inne / brak”. Dzięki temu wyniki analizy lotnisk można porównać według wielkości lotniska i stanu.

Pobrane strony są zapisywane w `data/scraped/` razem z datą pobrania, więc ponowne uruchomienie nie odpytuje Wikipedii (chyba że z opcją `--refresh`). Skrypt sprawdza poprawność i powtórzenia kodów lotnisk oraz dopasowuje dane do kodów z wyników analiz. Przewoźników, których w tabeli Wikipedii nie ma pod naszym kodem (Hawaiian — po połączeniu z Alaska Airlines strona podaje kod AS; Spirit), uzupełniamy ręcznie i oznaczamy jako `"source": "manual"`.

Wyniki:

- `results/airports.json` — wszystkie lotniska z tabeli,
- `results/carriers.json` — przewoźnicy z naszych danych,
- `results/scraping.md` — raport: liczba rekordów, problemy jakości, dopasowanie do naszych lotnisk i przewoźników.

## Testy

```powershell
python -B -m unittest discover -s tests -v
```

Testy obejmują pobieranie danych, import do HDFS i reguły czyszczenia, w tym CSV z przecinkami wewnątrz pól, fragmenty bez nagłówka, braki, błędne statusy oraz wartości liczbowe. Testy pobierania i importu korzystają z blokady plików Windows (`msvcrt`), więc uruchamia się je na Windows.
