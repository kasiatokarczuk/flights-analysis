# Raport jakości danych

Zakres: 2024-01 – 2025-09 (21 plików). Zadanie YARN: `application_1793906622171_0001`. Wynik: `/flights/clean` w HDFS.
Reguły czyszczenia opisano w README (sekcja o czyszczeniu danych).

## Rekordy

| | Liczba |
|---|---:|
| Wczytane | 12 321 982 |
| Odrzucone | 1 |
| Duplikaty | 0 |
| Zapisane | 12 321 981 |
| – w tym loty odwołane | 172 791 |
| – w tym loty przekierowane | 33 249 |

## Powody odrzucenia

| Powód | Liczba |
|---|---:|
| lot odbyty, ale bez opóźnienia przylotu | 1 |

## Braki i błędne wartości

Błędną wartość zastępujemy brakiem (pustym polem), nie zerem.

| Pole | Braki w źródle | Błędne → brak |
|---|---:|---:|
| CRSDepTime | 0 | 0 |
| DepDelay | 165 914 | 0 |
| ArrDelay | 206 040 | 0 |
| Distance | 0 | 0 |
| AirTime | 206 040 | 0 |
| CarrierDelay | 9 726 913 | 0 |
| WeatherDelay | 9 726 913 | 0 |
| NASDelay | 9 726 913 | 0 |
| SecurityDelay | 9 726 913 | 0 |
| LateAircraftDelay | 9 726 913 | 0 |

## Wartości podejrzane

| Kontrola | Liczba | Co robimy |
|---|---:|---|
| `AirTime` daje prędkość poza zakresem 100–700 mph | 331 | zamiana na brak |
| `ArrDelay` powyżej 24 godzin | 1 035 | zostawiamy, tylko liczymy |

Braki `ArrDelay` i `AirTime` dotyczą głównie lotów odwołanych i przekierowanych. Przyczyny opóźnień (`CarrierDelay` … `LateAircraftDelay`) są w źródle wypełniane tylko dla przylotów opóźnionych o co najmniej 15 minut, więc ich brak nie jest błędem.
