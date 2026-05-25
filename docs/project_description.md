# Opis projektu

## Temat

System monitorowania podejrzanych transakcji e-commerce w czasie rzeczywistym.

## Problem

Platformy e-commerce przetwarzają tysiące transakcji dziennie. Wśród nich zdarzają się oszustwa — transakcje dokonywane skradzionymi kartami, o nietypowych kwotach lub w podejrzanych godzinach. System batch-owy wykrywa je z opóźnieniem — nasz pipeline robi to natychmiast.

## Jak to działa

Producer symuluje ruch w sklepie internetowym i wysyła transakcje do Kafki. Spark Structured Streaming czyta je w czasie rzeczywistym, oblicza ryzyko na podstawie 4 reguł (wysoka kwota, elektronika, pora nocna, kombinacja cech) i zapisuje alerty do CSV. Dashboard Streamlit wizualizuje wyniki.

## Architektura

```
Producer → Kafka → Spark Streaming → CSV → Dashboard
```

Wszystko działa w Docker Compose — jedno polecenie uruchamia cały system.

## Reguły detekcji

Zastosowano scoring regułowy inspirowany podejściem z zajęć laboratoryjnych. Każda transakcja otrzymuje punkty za podejrzane cechy:

- kwota > 3000 PLN: +2 pkt
- kategoria elektronika: +1 pkt
- godziny nocne (0-5): +2 pkt
- elektronika z kwotą > 1000: +1 pkt

Transakcje z sumą >= 3 są klasyfikowane jako podejrzane.

## Możliwe rozszerzenia

- Model ML (Random Forest lub Isolation Forest)
- Powiadomienia e-mail/Slack
- Baza danych zamiast CSV
- Więcej reguł biznesowych
