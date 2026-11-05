# Raport jakości danych

Zakres: 2024-01 – 2025-09 (21 plików). Zadanie YARN: `application_1793055154276_0004`. Wynik: `/flights/clean` w HDFS.
Reguły czyszczenia opisano w README (sekcja o czyszczeniu danych).

## Rekordy

| | Liczba |
|---|---:|
| Wczytane | 12 321 982 |
| Odrzucone | 0 |
| Zapisane | 12 321 982 |
| – w tym loty odwołane | 172 791 |
| – w tym loty przekierowane | 33 249 |

## Braki i błędne wartości

Błędną wartość zastępujemy brakiem (pustym polem), nie zerem.

| Pole | Braki w źródle | Błędne → brak |
|---|---:|---:|
| CRSDepTime | 0 | 0 |
| DepDelay | 165 914 | 0 |
| ArrDelay | 206 041 | 0 |
| Distance | 0 | 0 |
| AirTime | 206 041 | 0 |
| CarrierDelay | 9 726 914 | 0 |
| WeatherDelay | 9 726 914 | 0 |
| NASDelay | 9 726 914 | 0 |
| SecurityDelay | 9 726 914 | 0 |
| LateAircraftDelay | 9 726 914 | 0 |

Braki `ArrDelay` i `AirTime` dotyczą głównie lotów odwołanych i przekierowanych. Przyczyny opóźnień (`CarrierDelay` … `LateAircraftDelay`) są w źródle wypełniane tylko dla przylotów opóźnionych o co najmniej 15 minut, więc ich brak nie jest błędem.
