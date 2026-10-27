# Sprawdzanie i czyszczenie danych

Zakres: 2024-01, 2024-02, 2024-03, 2024-04, 2024-05, 2024-06, 2024-07, 2024-08, 2024-09, 2024-10, 2024-11, 2024-12, 2025-01, 2025-02, 2025-03, 2025-04, 2025-05, 2025-06, 2025-07, 2025-08, 2025-09.

Zadanie YARN: `application_1793055154276_0004`, **FINISHED / SUCCEEDED**.
Dane oczyszczone: `/flights/clean`. Oryginały pozostają w `/flights/raw/`.

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
| Rekordy wejściowe | 12321982 |
| Zapisane rekordy | 12321982 |
| Pominięte nagłówki | 21 |
| Rekordy z usuniętymi spacjami | 0 |
| Rekordy ze statusem ujednoliconym do 0/1 | 12321982 |
| Kody lotnisk zmienione na wielkie litery | 0 |
| Godziny uzupełnione do HHMM | 0 |
| Odrzucone: struktura CSV | 0 |
| Odrzucone: data lub miesiąc | 0 |
| Odrzucone: brak przewoźnika | 0 |
| Odrzucone: kod lotniska | 0 |
| Odrzucone: identyfikator lotniska | 0 |
| Odrzucone: brak, błąd lub konflikt statusów | 0 |
| Nieodwołane i nieprzekierowane | 12115942 |
| Odwołane | 172791 |
| Przekierowane | 33249 |
| Brak ArrDelay: nieodwołane i nieprzekierowane | 1 |
| Brak ArrDelay: odwołane | 172791 |
| Brak ArrDelay: przekierowane | 33249 |

Statusy i braki pól liczbowych dotyczą zachowanych rekordów.

## Pola opcjonalne

| Pole | Pierwotne braki | Błędne wartości zamienione na brak |
|---|---:|---:|
| CRSDepTime | 0 | 0 |
| DepDelay | 165914 | 0 |
| ArrDelay | 206041 | 0 |
| Distance | 0 | 0 |
| AirTime | 206041 | 0 |
| CarrierDelay | 9726914 | 0 |
| WeatherDelay | 9726914 | 0 |
| NASDelay | 9726914 | 0 |
| SecurityDelay | 9726914 | 0 |
| LateAircraftDelay | 9726914 | 0 |

Brak przyczyny opóźnienia sam w sobie nie oznacza błędu. Źródło nie wymaga tych danych dla przylotów z opóźnieniem poniżej 15 minut ani dla lotów przekierowanych. Przyczyny odwołań są raportowane osobno.

## Rekordy według pliku źródłowego

| Miesiąc | Odczytane rekordy |
|---|---:|
| 2024-01 | 547271 |
| 2024-02 | 519221 |
| 2024-03 | 591767 |
| 2024-04 | 582185 |
| 2024-05 | 609743 |
| 2024-06 | 611132 |
| 2024-07 | 634613 |
| 2024-08 | 619025 |
| 2024-09 | 582622 |
| 2024-10 | 615497 |
| 2024-11 | 575404 |
| 2024-12 | 590581 |
| 2025-01 | 539747 |
| 2025-02 | 504884 |
| 2025-03 | 600872 |
| 2025-04 | 583950 |
| 2025-05 | 605648 |
| 2025-06 | 611575 |
| 2025-07 | 631428 |
| 2025-08 | 602378 |
| 2025-09 | 562439 |

Wszystkie liczebności miesięczne i liczba nagłówków są zgodne z manifestem. Zapisane i odrzucone rekordy rozliczają całe wejście.

## Schemat wyjścia i zakres kontroli

FlightDate, Reporting_Airline, Flight_Number_Reporting_Airline, OriginAirportID, Origin, OriginCityName, DestAirportID, Dest, DestCityName, CRSDepTime, DepDelay, ArrDelay, Cancelled, Diverted, Distance, AirTime, CarrierDelay, WeatherDelay, NASDelay, SecurityDelay, LateAircraftDelay

Pliki `part-*` są CSV bez nagłówka; kolejność kolumn opisuje `_schema.json` w katalogu wyjściowym. `_quality.json` zawiera liczniki i metadane.

To podstawowe sprawdzanie wybranych pól. Nie wykonano globalnego usuwania duplikatów ani kontroli pozostałych kolumn. Braków nie imputowano. Ten etap nie wykonuje analiz statystycznych.

Kod, manifest i aplikację identyfikują wartości w `data_quality.json`. Przebieg: `cleaning.log`.

[Zasady raportowania przyczyn opóźnień i odwołań](https://www.bts.gov/explore-topics-and-geography/modes/aviation/number-39-technical-directive-reporting-time).
