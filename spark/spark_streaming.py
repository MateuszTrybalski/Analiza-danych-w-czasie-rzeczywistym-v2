"""
Spark Structured Streaming — detekcja podejrzanych transakcji.

Pipeline:
  1. Czyta strumień JSON z Kafka (topik 'transactions')
  2. Parsuje dane transakcji
  3. Oblicza risk_score na podstawie prostych reguł biznesowych
  4. Filtruje podejrzane transakcje (risk_score >= 3)
  5. Zapisuje alerty do pliku CSV (append mode)
  6. Wypisuje alerty w konsoli

Reguły scoringu (z Ćwiczenia 1 — scoring regułowy):
  R1: amount > 3000              → +2 pkt
  R2: category == 'elektronika'  → +1 pkt
  R3: hour < 6 (noc)             → +2 pkt
  R4: amount > 1000 AND elektronika → +1 pkt
  Suma >= 3 → ALERT (transakcja podejrzana)

Bazuje na materiałach z Ćwiczeń 1 i 3.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, when, lit, current_timestamp, concat_ws
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)

# ---------------------------------------------------------------------------
# Konfiguracja
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9092")
INPUT_TOPIC = "transactions"
ALERTS_CSV_PATH = "/data/alerts.csv"
RISK_THRESHOLD = 3  # minimalna suma punktów ryzyka dla alertu

# ---------------------------------------------------------------------------
# Schemat danych transakcji (musi odpowiadać producerowi)
# ---------------------------------------------------------------------------
TRANSACTION_SCHEMA = StructType([
    StructField("tx_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("category", StringType(), True),
    StructField("store", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("hour", IntegerType(), True),
    StructField("timestamp", StringType(), True),
])


def main():
    # ----- Sesja Spark -----
    spark = (
        SparkSession.builder
        .appName("FraudDetection-Streaming")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print("[SPARK] Sesja Spark uruchomiona")

    # ----- Czytanie strumienia z Kafka -----
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", INPUT_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )
    print(f"[SPARK] Subskrybuję topik '{INPUT_TOPIC}'")

    # ----- Parsowanie JSON -----
    transactions = (
        raw_stream
        .selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), TRANSACTION_SCHEMA).alias("tx"))
        .select("tx.*")
    )

    # ----- Reguły scoringu (biznesowe) -----
    scored = transactions.withColumn(
        "rule_high_amount",
        when(col("amount") > 3000, lit(2)).otherwise(lit(0))
    ).withColumn(
        "rule_electronics",
        when(col("category") == "elektronika", lit(1)).otherwise(lit(0))
    ).withColumn(
        "rule_night",
        when(col("hour") < 6, lit(2)).otherwise(lit(0))
    ).withColumn(
        "rule_electronics_high",
        when(
            (col("amount") > 1000) & (col("category") == "elektronika"),
            lit(1)
        ).otherwise(lit(0))
    )

    # Suma punktów ryzyka
    scored = scored.withColumn(
        "risk_score",
        col("rule_high_amount") + col("rule_electronics") +
        col("rule_night") + col("rule_electronics_high")
    )

    # Poziom ryzyka
    scored = scored.withColumn(
        "risk_level",
        when(col("risk_score") >= 5, lit("CRITICAL"))
        .when(col("risk_score") >= 3, lit("HIGH"))
        .when(col("risk_score") >= 1, lit("MEDIUM"))
        .otherwise(lit("LOW"))
    )

    # Lista aktywowanych reguł (czytelna kolumna)
    scored = scored.withColumn(
        "triggered_rules",
        concat_ws(", ",
            when(col("rule_high_amount") > 0, lit("HIGH_AMOUNT")),
            when(col("rule_electronics") > 0, lit("ELECTRONICS")),
            when(col("rule_night") > 0, lit("NIGHT_TX")),
            when(col("rule_electronics_high") > 0, lit("ELEC+HIGH_AMT")),
        )
    )

    # Timestamp przetworzenia
    scored = scored.withColumn("processed_at", current_timestamp())

    # ----- Filtrowanie alertów -----
    alerts = scored.filter(col("risk_score") >= RISK_THRESHOLD)

    # Kolumny do zapisu
    output_columns = [
        "tx_id", "user_id", "amount", "category", "store",
        "payment_method", "hour", "timestamp",
        "risk_score", "risk_level", "triggered_rules", "processed_at"
    ]

    # ----- Strumień 1: Konsola (podgląd alertów) -----
    console_query = (
        alerts.select(*output_columns)
        .writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", "false")
        .option("numRows", 50)
        .queryName("alerts-console")
        .start()
    )
    print("[SPARK] Strumień konsoli uruchomiony")

    # ----- Strumień 2: CSV (persystencja alertów) -----
    csv_query = (
        alerts.select(*output_columns)
        .writeStream
        .outputMode("append")
        .format("csv")
        .option("path", ALERTS_CSV_PATH)
        .option("checkpointLocation", "/data/_checkpoint-alerts")
        .option("header", "true")
        .queryName("alerts-csv")
        .start()
    )
    print(f"[SPARK] Strumień CSV uruchomiony → {ALERTS_CSV_PATH}")

    # ----- Czekaj na zakończenie -----
    print("[SPARK] Pipeline działa. Ctrl+C aby zatrzymać.")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
