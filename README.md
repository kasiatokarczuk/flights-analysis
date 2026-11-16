# Big Data — analiza lotów w USA

Projekt analizuje opóźnienia i odwołania lotów krajowych w USA. Dane BTS (5,57 GB) są przechowywane w HDFS i przetwarzane w Hadoop MapReduce (Hadoop Streaming), dane o lotniskach i przewoźnikach pochodzą ze scrapingu Wikipedii, a wyniki trafiają do dwóch baz MongoDB: lokalnej (Docker) i MongoDB Atlas. Wyniki pokazuje panel w przeglądarce. Kod jest napisany w Pythonie, a klaster Hadoop i lokalna baza działają w Dockerze.

## Dane

Źródło: **Reporting Carrier On-Time Performance (1987–present)**, Bureau of Transportation Statistics.

- [Opis zbioru](https://www.transtats.bts.gov/tables.asp?QO_VQ=EFD&QO_anzr=Nv4yv0r)
- [Opis kolumn](https://www.transtats.bts.gov/Fields.asp?gnoyr_VQ=FGJ)
- [Archiwa miesięczne ZIP](https://transtats.bts.gov/PREZIP/)

Pobrany zbiór obejmuje styczeń 2024 – wrzesień 2025: **21 plików CSV, 12 321 982 rekordy i 5,57 GB po rozpakowaniu**. Jeden rekord opisuje jeden lot. Pliki zawierają m.in. datę, przewoźnika, lotniska, opóźnienia oraz oznaczenia odwołania i przekierowania.

## Wymagania

Windows, Python 3.11 lub nowszy oraz uruchomiony Docker Desktop z Docker Compose. Biblioteki instalujemy w osobnym środowisku wirtualnym projektu, żeby nie zmieniały wersji pakietów używanych przez inne projekty:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Przed uruchamianiem skryptów środowisko trzeba aktywować (`.venv\Scripts\Activate.ps1`). Jeśli PowerShell blokuje aktywację, wystarczy raz wykonać `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`.

Poniższe polecenia należy wykonywać w głównym folderze projektu.

## Kolejność uruchamiania

Pierwsze uruchomienie (szczegóły w kolejnych sekcjach):

```powershell
.venv\Scripts\Activate.ps1
copy .env.example .env                     # jednorazowo: własne hasła i adres Atlas
python scripts/make_certs.py               # jednorazowo: certyfikaty TLS dla MongoDB
docker compose up -d                       # Hadoop (HDFS, YARN, historia zadań) i MongoDB

python scripts/download_flights.py         # 1. pobranie danych BTS do data/
python scripts/import_hdfs.py              # 2. kopia do HDFS /flights/raw
python scripts/run_cleaning.py             # 3. czyszczenie w MapReduce -> /flights/clean
python scripts/run_analysis.py             # 4. analizy w MapReduce -> /flights/analysis
python scripts/scrape_wikipedia.py         # 5. scraping lotnisk i przewoźników
python scripts/db.py local                 # 6. test połączenia z bazami
python scripts/db.py atlas
python scripts/load_mongo.py               # 7. import do obu baz MongoDB
streamlit run scripts/dashboard.py         # 8. panel w przeglądarce
```

Każdy krok można powtórzyć bez szkody dla danych: pobieranie i import do HDFS pomijają gotowe pliki, zadania MapReduce nie nadpisują istniejących wyników, a import do MongoDB aktualizuje dokumenty zamiast je powielać.

## Struktura projektu

| Ścieżka | Zawartość |
|---|---|
| `scripts/download_flights.py`, `import_hdfs.py` | pobieranie danych BTS i kopia do HDFS |
| `scripts/clean_flights.py`, `run_cleaning.py` | czyszczenie danych w MapReduce (mapper i reducer, uruchomienie) |
| `scripts/analyze_flights.py`, `run_analysis.py` | analizy statystyczne w MapReduce |
| `scripts/common.py` | wspólne funkcje uruchamiania poleceń i zadań Hadoop |
| `scripts/scrape_wikipedia.py` | web scraping Wikipedii |
| `scripts/make_certs.py`, `db.py`, `load_mongo.py` | certyfikaty TLS, połączenia i import do MongoDB |
| `scripts/queries.py`, `charts.py`, `dashboard.py` | zapytania, wykresy i panel |
| `tests/` | testy jednostkowe |
| `compose.yaml`, `infra/` | konfiguracja kontenerów Hadoop i MongoDB |
| `data/manifest.json` | opis pobranych plików (źródło, liczba rekordów, SHA-256); same dane nie są w repozytorium |
| `results/` | raporty jakości, wyniki analiz, dane ze scrapingu, wykresy |

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

## Lokalna MongoDB (TLS i logowanie)

Baza działa w kontenerze `mongodb` i przyjmuje wyłącznie połączenia szyfrowane (`--tlsMode requireTLS`) od zalogowanych użytkowników (`--auth`). Przygotowanie (jednorazowo):

```powershell
copy .env.example .env               # następnie wpisz własne hasła w .env
python scripts/make_certs.py         # certyfikaty TLS w secrets/tls/
docker compose up -d mongodb
python scripts/db.py local           # test połączenia i zabezpieczeń
```

- `make_certs.py` tworzy własny urząd certyfikacji (`ca.pem`) i podpisany nim certyfikat serwera (`server.pem`) dla nazw `localhost` i `mongodb`. Klient ufa serwerowi, bo zna `ca.pem`; własnego certyfikatu klient nie potrzebuje.
- Przy pierwszym starcie pustej bazy powstają dwa konta: administrator (`MONGO_ROOT_USER`) i użytkownik aplikacji (`MONGO_APP_USER`) z prawem odczytu i zapisu wyłącznie bazy `flights` (`infra/mongo-init.js`). Skrypty projektu łączą się jako użytkownik aplikacji. Zmiana haseł w `.env` po pierwszym starcie nie zmienia kont w istniejącej bazie.
- `db.py local` łączy się z bazą, a potem sprawdza, że serwer odrzuca połączenie bez TLS i logowanie złym hasłem.
- Pliki `.env` i `secrets/` nie trafiają do repozytorium (`.gitignore`). W hasłach nie używaj znaku `$` — Docker Compose traktuje go jako początek zmiennej.

Połączenie w MongoDB Compass: *New connection → Advanced Connection Options*:

- *General*: Host `localhost:27017`,
- *Authentication*: Username/Password, użytkownik i hasło z `MONGO_APP_USER` / `MONGO_APP_PASSWORD`, *Authentication Database* `flights`,
- *TLS/SSL*: *SSL/TLS Connection* `On`, *Certificate Authority (.pem)* — plik `secrets\tls\ca.pem`.

## Import do MongoDB (lokalna baza i Atlas)

```powershell
python scripts/load_mongo.py                 # obie bazy
python scripts/load_mongo.py --target local  # tylko jedna: local albo atlas
python scripts/load_mongo.py --no-sample     # bez próbki lotów
python scripts/db.py atlas                   # test połączenia z Atlas
```

Adres Atlas (`ATLAS_URI`) jest w pliku `.env`. Atlas szyfruje połączenia (TLS) i wymaga logowania domyślnie; dostęp mają tylko adresy IP dodane w *Network Access*.

Kolekcje w bazie `flights`:

| Kolekcja | Zawartość | `_id` |
|---|---|---|
| `analysis` | wyniki analiz MapReduce, jeden dokument na grupę | np. `carrier|WN`, `airport|ATL`, `total|all` |
| `airports` | lotniska ze scrapingu Wikipedii | kod lotniska w danych BTS (np. `PBI`) |
| `carriers` | przewoźnicy ze scrapingu | kod IATA (np. `9E`) |
| `metadata` | manifest danych, raport jakości, informacje o analizie i scrapingu | `manifest`, `cleaning`, `analysis`, `scraping` |
| `import_log` | jeden wpis na każde uruchomienie importu z liczbą zmian | automatyczny |
| `flights_sample` | oczyszczone loty z września 2025 czytane z HDFS (tylko baza lokalna) | klucz lotu, np. `2025-09-01|9E|4800|CHS|JFK|0700` |

Główny zbiór danych pozostaje w HDFS; do MongoDB trafiają wyniki analiz, dane ze scrapingu, metadane i próbka lotów do przeglądania. Próbki nie ładujemy do Atlas, bo darmowy klaster ma limit 512 MB.

Ponowne uruchomienie nie powiela danych: każdy dokument ma stały `_id` i jest zapisywany operacją *zastąp albo wstaw* (upsert). Dokumenty, których nie ma już w plikach źródłowych, są usuwane. Skrypt wypisuje dla każdej kolekcji liczbę dokumentów nowych, zmienionych, bez zmian i usuniętych, a po zapisie sprawdza, czy liczba dokumentów w bazie zgadza się ze źródłem. Przy drugim uruchomieniu wszystkie dokumenty są „bez zmian”.

## Wizualizacja: panel i wykresy

```powershell
streamlit run scripts/dashboard.py   # panel w przeglądarce (http://localhost:8501)
python scripts/charts.py             # wykresy PNG do results/charts/ (opcja --target atlas)
```

Panel czyta dane na bieżąco z MongoDB; na pasku bocznym wybiera się bazę (lokalna albo Atlas). Zakładki:

- **Przegląd** — liczba lotów, odsetek lotów na czas, średnie i mediana opóźnienia, odsetek odwołań, zakres danych,
- **Przewoźnicy** — punktualność z nazwami i typem przewoźnika ze scrapingu, do wyboru miara,
- **Sezonowość** — miesiące i dni tygodnia,
- **Pora dnia** — odsetek opóźnionych odlotów według godziny planowanego odlotu,
- **Przyczyny** — udział pięciu przyczyn opóźnień, łącznie albo dla wybranego przewoźnika,
- **Lotniska** — wyniki lotnisk złączone (`$lookup`) z danymi ze scrapingu: porównanie według wielkości lotniska (kategoria FAA) i według stanów,
- **Próbka lotów** — filtrowanie lotów z września 2025 (przewoźnik, lotnisko, odwołane); podsumowanie liczy MongoDB (`$group`). Tylko baza lokalna.

Zapytania są w `queries.py`, a wykresy (matplotlib) w `charts.py`. Panel i skrypt PNG korzystają z tych samych funkcji, więc wykresy w sprawozdaniu są takie same jak w panelu. Odsetki dla grup lotnisk (wielkość, stan) są średnimi ważonymi liczbą lotów.

## Testy

```powershell
python -B -m unittest discover -s tests -v
```

Testy (65) obejmują:

- pobieranie danych, wznawianie przerwanego pobierania i import do HDFS z kontrolą sum,
- reguły czyszczenia (pola z przecinkami, fragmenty bez nagłówka, braki, błędne wartości) i usuwanie duplikatów,
- statystyki analiz, w tym medianę i niezależność wyniku od podziału danych między mappery,
- parsowanie tabel Wikipedii i dopasowanie kodów lotnisk i przewoźników,
- certyfikaty TLS i odczyt pliku `.env`,
- import do MongoDB (ponowny import bez duplikatów, usuwanie nieaktualnych danych) i zapytania z `$lookup`.

Testy pobierania i importu korzystają z blokady plików Windows (`msvcrt`), więc uruchamia się je na Windows. Import do MongoDB testujemy na bazie działającej w pamięci (`mongomock`), bez serwera.
