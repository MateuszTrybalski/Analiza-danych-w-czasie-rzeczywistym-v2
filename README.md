# System monitorowania podejrzanych transakcji e-commerce

Projekt zaliczeniowy — **Analiza danych w czasie rzeczywistym**

---

## Problem biznesowy

Sklepy internetowe tracą miliony złotych rocznie z powodu oszustw transakcyjnych.
Tradycyjne systemy wykrywają nieprawidłowości z opóźnieniem godzin lub dni.
Ten projekt implementuje pipeline, który analizuje transakcje **w momencie ich wystąpienia**
i natychmiast generuje alerty.

## Architektura

```
Producer ──▶ Kafka ──▶ Spark Streaming ──▶ CSV (alerty)
  (Python)    (broker)    (scoring reguł)       │
                                                ▼
                                          Dashboard
                                          (Streamlit)
```

1. **Producer** generuje syntetyczne transakcje (~10 % to potencjalne fraudy)
2. **Kafka** buforuje strumień w topiku `transactions`
3. **Spark Structured Streaming** czyta strumień, liczy `risk_score`, filtruje podejrzane
4. Alerty (`risk_score ≥ 3`) trafiają do **CSV** i na **konsolę**
5. **Dashboard Streamlit** prezentuje wykresy i tabelę alertów

## Reguły scoringowe

| # | Warunek | Punkty |
|---|---------|--------|
| R1 | `amount > 3000` | +2 |
| R2 | `category = elektronika` | +1 |
| R3 | `hour < 6` (transakcja nocna) | +2 |
| R4 | `amount > 1000` **i** `elektronika` | +1 |

Suma ≥ 5 → **CRITICAL** · ≥ 3 → **HIGH** · ≥ 1 → **MEDIUM**

## Technologie

| Komponent | Technologia |
|-----------|-------------|
| Message broker | Apache Kafka 7.5 (Confluent) |
| Stream processing | Spark Structured Streaming 3.5 |
| Język | Python 3.11 |
| Dashboard | Streamlit 1.31 + Plotly |
| Konteneryzacja | Docker Compose |

## Struktura projektu

```
├── docker-compose.yml
├── requirements.txt
├── README.md
├── producer/
│   ├── producer.py
│   └── Dockerfile
├── spark/
│   ├── spark_streaming.py
│   └── Dockerfile
├── dashboard/
│   ├── dashboard.py
│   └── Dockerfile
├── notebooks/
│   └── analysis.ipynb
├── data/                 ← generowane przez Spark
└── docs/
    ├── project_description.md
    └── presentation_notes.md
```

## Uruchomienie

### Wymagania

- Docker Desktop (Windows / Mac) lub Docker + Docker Compose (Linux)
- ~4 GB wolnego RAM

### Start

```bash
git clone https://github.com/MateuszTrybalski/Analiza-danych-w-czasie-rzeczywistym-v2.git
cd Analiza-danych-w-czasie-rzeczywistym-v2

docker compose up --build
```

Poczekaj ok. 60 sekund. W logach zobaczysz:

```
producer        | [PRODUCER] Polaczono z Kafka (broker:9092)
spark-streaming | [SPARK] Pipeline dziala. Ctrl+C aby zatrzymac.
```

### Dostęp

| Serwis | URL |
|--------|-----|
| Dashboard | http://localhost:8501 |
| Spark UI | http://localhost:8080 |
| Spark Job | http://localhost:4040 |

### Zatrzymanie

```bash
docker compose down
```

## Przykładowe alerty

```
tx_id    | amount  | category    | store    | hour | risk_score | risk_level | triggered_rules
TX34521  | 4523.50 | elektronika | Warszawa |  3   |     6      | CRITICAL   | HIGH_AMOUNT, ELECTRONICS, NIGHT_TX, ELEC+HIGH_AMT
TX78123  | 3890.00 | sport       | Krakow   |  2   |     4      | HIGH       | HIGH_AMOUNT, NIGHT_TX
TX12456  | 5200.00 | elektronika | Gdansk   | 14   |     4      | HIGH       | HIGH_AMOUNT, ELECTRONICS, ELEC+HIGH_AMT
```

## Demo (krok po kroku)

1. `docker compose up --build`
2. Logi producenta: `docker logs -f producer`
3. Logi Spark (alerty): `docker logs -f spark-streaming`
4. Dashboard: http://localhost:8501
5. Spark UI: http://localhost:8080

---

Projekt wykonany w ramach przedmiotu *Analiza danych w czasie rzeczywistym*.
