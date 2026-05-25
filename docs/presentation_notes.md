# Notatki do prezentacji

## Plan prezentacji (~10–15 min)

### 1. Wprowadzenie (1 min)

- "Nasz projekt to system wykrywania podejrzanych transakcji w sklepie internetowym"
- "Problem: oszustwa trzeba wykrywać natychmiast, nie po kilku godzinach"
- "Rozwiązanie: pipeline w czasie rzeczywistym z Kafką i Sparkiem"

### 2. Architektura (2 min)

- "Pipeline jest prosty: Producer generuje transakcje → Kafka je kolejkuje → Spark analizuje → alerty trafiają do dashboardu"
- "Wszystko działa w Dockerze — jedno polecenie i cały system startuje"

### 3. Reguły (1 min)

- "Stosujemy 4 reguły: wysoka kwota, elektronika, godziny nocne, kombinacja"
- "Każda reguła daje punkty. Suma >= 3 oznacza alert"

### 4. Demo na żywo (5–7 min)

```bash
# Krok 1 — uruchom stack
docker compose up --build

# Krok 2 — pokaż producenta
docker logs -f producer

# Krok 3 — pokaż alerty Spark
docker logs -f spark-streaming

# Krok 4 — otwórz dashboard
# http://localhost:8501

# Krok 5 (opcja) — Spark UI
# http://localhost:8080
```

### 5. Podsumowanie (1 min)

- "System działa end-to-end w Docker Compose"
- "Bazuje na koncepcjach z laboratoriów — scoring regułowy"
- "Możliwe rozszerzenia: model ML, alerty e-mail, baza danych"

## Checklista przed demo

- [ ] Docker Desktop działa
- [ ] `docker compose up --build` przechodzi bez błędów
- [ ] Producer generuje transakcje (widoczne w logach)
- [ ] Spark wykrywa alerty (widoczne w logach)
- [ ] Dashboard otwiera się na http://localhost:8501
- [ ] Dashboard pokazuje dane (wykresy, tabela)

## Pytania prowadzącego — gotowe odpowiedzi

**Dlaczego Kafka a nie bezpośredni strumień?**
Kafka buforuje dane. Jeśli Spark się zrestartuje — nie tracimy transakcji.

**Dlaczego reguły a nie ML?**
Reguły są prostsze i wyjaśnialne. Na laboratoriach poznaliśmy też Random Forest i Isolation Forest — można je łatwo dodać.

**Jak skalować?**
Kafka — więcej partycji. Spark — więcej workerów. Producer — wiele instancji.

**Skąd dane?**
Syntetyczny generator. W produkcji byłoby to API sklepu lub system płatności.
