"""
Producent transakcji e-commerce dla Apache Kafka.

Generuje syntetyczne transakcje zakupowe i wysyła je do topiku 'transactions'.
Około 10% transakcji to podejrzane transakcje (potencjalne fraudy):
  - wysokie kwoty (>3000 PLN)
  - kategoria 'elektronika'
  - godziny nocne (0-5)

Bazuje na materiale z Ćwiczenia 1 (producent Kafka).
"""

from kafka import KafkaProducer
import json
import random
import time
import os
from datetime import datetime, timedelta

# ---------------------------------------------------------------------------
# Konfiguracja
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9092")
TOPIC = "transactions"
SEND_INTERVAL = 1.0          # sekundy między transakcjami
FRAUD_RATIO = 0.10            # ~10% transakcji to fraudy

# Dane referencyjne
SKLEPY = ["Warszawa", "Kraków", "Gdańsk", "Wrocław", "Poznań"]
KATEGORIE = ["elektronika", "odzież", "żywność", "książki", "sport"]
METODY_PLATNOSCI = ["karta", "blik", "przelew", "gotówka"]


def create_producer() -> KafkaProducer:
    """Tworzy instancję KafkaProducer z retry przy starcie."""
    for attempt in range(30):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print(f"[PRODUCER] Połączono z Kafka ({KAFKA_BOOTSTRAP})")
            return producer
        except Exception as e:
            print(f"[PRODUCER] Próba {attempt+1}/30 — Kafka niedostępna: {e}")
            time.sleep(2)
    raise ConnectionError("Nie udało się połączyć z Kafka!")


def generate_transaction() -> dict:
    """
    Generuje pojedynczą transakcję.
    Z prawdopodobieństwem FRAUD_RATIO generuje transakcję podejrzaną.
    """
    now = datetime.now()
    tx_id = f"TX{random.randint(10000, 99999)}"
    user_id = f"u{random.randint(1, 50):03d}"

    if random.random() < FRAUD_RATIO:
        # ---------- Transakcja PODEJRZANA ----------
        amount = round(random.uniform(3000, 9000), 2)
        category = random.choice(["elektronika", "elektronika", "elektronika", "sport"])
        hour = random.randint(0, 5)  # godziny nocne
        store = random.choice(SKLEPY)
        payment = random.choice(METODY_PLATNOSCI)
    else:
        # ---------- Transakcja NORMALNA ----------
        amount = round(random.lognormvariate(5, 1), 2)
        amount = max(5.0, min(amount, 3000.0))  # clip do 5-3000
        category = random.choice(KATEGORIE)
        hour = random.randint(6, 23)
        store = random.choice(SKLEPY)
        payment = random.choice(METODY_PLATNOSCI)

    # Budujemy timestamp z wybraną godziną (dzisiaj)
    ts = now.replace(hour=hour, minute=random.randint(0, 59),
                     second=random.randint(0, 59), microsecond=0)

    return {
        "tx_id": tx_id,
        "user_id": user_id,
        "amount": amount,
        "category": category,
        "store": store,
        "payment_method": payment,
        "hour": hour,
        "timestamp": ts.isoformat(),
    }


def main():
    """Główna pętla producenta."""
    producer = create_producer()
    print(f"[PRODUCER] Wysyłam transakcje do topiku '{TOPIC}' co {SEND_INTERVAL}s")
    print(f"[PRODUCER] ~{FRAUD_RATIO*100:.0f}% transakcji to potencjalne fraudy")
    print("-" * 70)

    count = 0
    try:
        while True:
            tx = generate_transaction()
            producer.send(TOPIC, value=tx)
            count += 1

            # Kolorowy output w terminalu
            is_suspicious = tx["amount"] > 3000 or tx["hour"] < 6
            marker = "🔴 FRAUD?" if is_suspicious else "🟢 OK"
            print(
                f"[{count:>5}] {marker} {tx['tx_id']} | "
                f"{tx['amount']:>8.2f} PLN | {tx['category']:<12} | "
                f"{tx['store']:<10} | godz. {tx['hour']:02d}"
            )

            time.sleep(SEND_INTERVAL)

    except KeyboardInterrupt:
        print(f"\n[PRODUCER] Zatrzymano. Wysłano {count} transakcji.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
