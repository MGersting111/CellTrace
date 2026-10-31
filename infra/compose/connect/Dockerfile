FROM confluentinc/cp-kafka-connect:8.0.8

USER root
RUN mkdir -p /usr/share/confluent-hub-components/debezium-postgres \
 && curl -fsSL https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/3.7.0.Final/debezium-connector-postgres-3.7.0.Final-plugin.tar.gz \
    | tar -xz -C /usr/share/confluent-hub-components/debezium-postgres --strip-components=1
USER appuser
