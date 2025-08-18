ARG TRINO_VERSION=475

FROM trinodb/trino:${TRINO_VERSION}

USER root

COPY ./certs/ExnessRootCA.crt /usr/local/share/ca-certificates/
COPY ./certs/ExnessServerCA.crt /usr/local/share/ca-certificates/
COPY ./certs/combined.crt /usr/local/share/ca-certificates/
COPY ./certs/NewExnessIssuedCA.crt /usr/local/share/ca-certificates/
COPY ./certs/NewExnessRootCA.crt /usr/local/share/ca-certificates/
COPY ./certs/combined.crt /etc/ssl/certs/ca-certificates.crt




RUN keytool -import -trustcacerts -alias exness-ca -file /usr/local/share/ca-certificates/ExnessRootCA.crt -cacerts -storepass changeit -noprompt && \
    keytool -import -trustcacerts -alias exness-srv -file /usr/local/share/ca-certificates/ExnessServerCA.crt -cacerts -storepass changeit -noprompt && \
    keytool -import -trustcacerts -alias new-exness-ca -file /usr/local/share/ca-certificates/NewExnessRootCA.crt -cacerts -storepass changeit -noprompt && \
    keytool -import -trustcacerts -alias new-exness-issued-ca -file /usr/local/share/ca-certificates/NewExnessIssuedCA.crt -cacerts -storepass changeit -noprompt && \
    rm -rf /usr/lib/trino/plugin/postgresql/* && \
    rm -rf /usr/lib/trino/plugin/vertica/* && \
    rm -rf /usr/lib/trino/plugin/kafka/* && \
    rm -rf /usr/lib/trino/plugin/delta-lake/* && \
    rm -rf /usr/lib/trino/plugin/iceberg/* && \
    rm -rf /usr/lib/trino/lib/io.trino_trino-main-475.jar

COPY vertica /usr/lib/trino/plugin/vertica
COPY postgresql /usr/lib/trino/plugin/postgresql
COPY kafka /usr/lib/trino/plugin/kafka
COPY delta /usr/lib/trino/plugin/delta-lake
COPY iceberg /usr/lib/trino/plugin/iceberg
COPY lib/io.trino_trino-main-475.jar /usr/lib/trino/lib/

RUN mkdir -p /etc/kafka
COPY kafka_table_descriptions/ /etc/kafka/

RUN mkdir -p /etc/kafka_certs
COPY kafka_certs/ /etc/kafka_certs/

RUN mkdir -p /usr/lib/trino/plugin/storage
COPY trino-storage/ /usr/lib/trino/plugin/storage/


#chmod +r /keystores/truststore.jks && \p
RUN chmod -R +r /usr/lib/trino/plugin/vertica && \
    chmod -R +r /usr/lib/trino/plugin/kafka && \
    chmod -R +r /usr/lib/trino/plugin/storage && \
    chmod -R +r /usr/lib/trino/plugin/delta-lake && \
    chmod -R +r /usr/lib/trino/plugin/iceberg && \
    chmod -R +r /etc/kafka && \
    chmod -R +r /etc/kafka_certs && \
    chmod -R +r /usr/lib/trino/plugin/postgresql && \
    mkdir -p /opt/java/openjdk/lib/security \
    && chmod --reference=/usr/lib/jvm/temurin/jdk-24+36/lib/security /opt/java/openjdk/lib/security \
    && ln -s /usr/lib/jvm/temurin/jdk-24+36/lib/security/cacerts /opt/java/openjdk/lib/security/cacerts


COPY other_plugins/jwt-trino-plugin-1.0-SNAPSHOT.jar \
     /usr/lib/trino/plugin/jwt/

COPY other_plugins/trino-api-plugin-1.0-SNAPSHOT.jar \
     /usr/lib/trino/plugin/api/

COPY other_plugins/trino-convert-to-western-digits-plugin-1.0-SNAPSHOT.jar \
     /usr/lib/trino/plugin/convert-to-western-digits/

COPY other_plugins/trino-random-functions-1.0.0.jar \
     /usr/lib/trino/plugin/random-functions/

COPY other_plugins/trino-big-sequence-1.0.0.jar \
     /usr/lib/trino/plugin/big-sequence/

RUN chmod -R a+r /usr/lib/trino/plugin/jwt \
                 /usr/lib/trino/plugin/api \
                 /usr/lib/trino/plugin/random-functions \
                 /usr/lib/trino/plugin/big-sequence \
                 /usr/lib/trino/plugin/convert-to-western-digits

USER trino:trino