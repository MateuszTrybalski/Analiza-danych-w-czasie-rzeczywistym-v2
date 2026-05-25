FROM bitnami/spark:3.5

USER root

# Pobieramy JARy Kafki przy BUDOWANIU obrazu
# Dzieki temu nie trzeba pobierac ich przy starcie (problem z Ivy cache znika)
RUN mkdir -p /opt/spark-extra-jars && \
    cd /opt/spark-extra-jars && \
    curl -fSL -o spark-sql-kafka.jar \
      "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar" && \
    curl -fSL -o kafka-clients.jar \
      "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar" && \
    curl -fSL -o spark-token-provider.jar \
      "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar" && \
    curl -fSL -o commons-pool2.jar \
      "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar"

COPY spark_streaming.py /app/spark_streaming.py

USER 1001

CMD ["spark-submit", \
     "--master", "spark://spark-master:7077", \
     "--jars", "/opt/spark-extra-jars/spark-sql-kafka.jar,/opt/spark-extra-jars/kafka-clients.jar,/opt/spark-extra-jars/spark-token-provider.jar,/opt/spark-extra-jars/commons-pool2.jar", \
     "/app/spark_streaming.py"]
