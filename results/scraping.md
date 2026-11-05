# Raport ze scrapingu Wikipedii

- Lotniska: [https://en.wikipedia.org/wiki/List_of_airports_in_the_United_States](https://en.wikipedia.org/wiki/List_of_airports_in_the_United_States), liczba wierszy: 389
- Przewoźnicy: [https://en.wikipedia.org/wiki/List_of_airlines_of_the_United_States](https://en.wikipedia.org/wiki/List_of_airlines_of_the_United_States), liczba wierszy: 144

## Jakość danych

- Brak problemów z kodami lotnisk.

## Dopasowanie lotnisk do naszych danych

Znaleziono 339 z 355 lotnisk z analiz.

| Wielkość (kategoria FAA) | Lotniska |
|---|---:|
| duże | 31 |
| średnie | 32 |
| małe | 80 |
| bardzo małe | 196 |
| inne / brak | 0 |

Brak w tabeli Wikipedii (16): ADK, BIH, CLD, DDC, DVL, FMN, FOD, GUF, INL, LAF, LBL, MCW, MGW, PPG, VCT, VEL

Kody różne na Wikipedii i w danych BTS (Wikipedia → BTS): DJT → PBI

## Dopasowanie przewoźników

| Kod | Nazwa | Typ | Lata jako | Źródło |
|---|---|---|---|---|
| 9E | Endeavor Air | regional | Delta Connection | wikipedia |
| AA | American Airlines | mainline | – | wikipedia |
| AS | Alaska Airlines | mainline | – | wikipedia |
| B6 | JetBlue | mainline | – | wikipedia |
| DL | Delta Air Lines | mainline | – | wikipedia |
| F9 | Frontier Airlines | mainline | – | wikipedia |
| G4 | Allegiant Air | mainline | – | wikipedia |
| HA | Hawaiian Airlines | mainline | – | manual |
| MQ | Envoy Air | regional | American Eagle | wikipedia |
| NK | Spirit Airlines | mainline | – | manual |
| OH | PSA Airlines | regional | American Eagle | wikipedia |
| OO | SkyWest Airlines | regional | Delta Connection | wikipedia |
| UA | United Airlines | mainline | – | wikipedia |
| WN | Southwest Airlines | mainline | – | wikipedia |
| YX | Republic Airways | regional | American Eagle | wikipedia |

- AS: kilku kandydatów (Alaska Airlines, Hawaiian Airlines), wybrano pierwszego
- HA: brak w tabeli Wikipedii, uzupełniono ręcznie (Hawaiian Airlines)
- NK: brak w tabeli Wikipedii, uzupełniono ręcznie (Spirit Airlines)
