# Build argument for Trino version
ARG TRINO_VERSION=475

# Stage 1: Build Trino from source
FROM eclipse-temurin:23-jdk AS builder

# Set working directory
WORKDIR /build

# Copy the entire Trino source code (assuming Dockerfile is in image/ folder)
COPY ../ /build/

# Build Trino with Maven wrapper
RUN ./mvnw clean install -DskipTests

# Create a staging directory for artifacts we'll need
RUN mkdir -p /artifacts/plugins && \
    mkdir -p /artifacts/lib

# Copy built plugins to artifacts directory
# Each plugin's target directory contains trino-{plugin}-{version}/ with JARs
RUN cp -r /build/plugin/trino-postgresql/target/trino-postgresql-${TRINO_VERSION}/* /artifacts/plugins/postgresql/ && \
    cp -r /build/plugin/trino-vertica/target/trino-vertica-${TRINO_VERSION}/* /artifacts/plugins/vertica/ && \
    cp -r /build/plugin/trino-kafka/target/trino-kafka-${TRINO_VERSION}/* /artifacts/plugins/kafka/ && \
    cp -r /build/plugin/trino-delta-lake/target/trino-delta-lake-${TRINO_VERSION}/* /artifacts/plugins/delta-lake/ && \
    cp -r /build/plugin/trino-iceberg/target/trino-iceberg-${TRINO_VERSION}/* /artifacts/plugins/iceberg/

# Copy the main jar from core
RUN cp /build/core/trino-main/target/trino-main-${TRINO_VERSION}.jar /artifacts/lib/io.trino_trino-main-${TRINO_VERSION}.jar

# Stage 2: Build final Trino image
FROM trinodb/trino:${TRINO_VERSION}

# Switch to root for installation
USER root

# Copy certificates
COPY ./certs/ExnessRootCA.crt /usr/local/share/ca-certificates/
COPY ./certs/ExnessServerCA.crt /usr/local/share/ca-certificates/
COPY ./certs/combined.crt /usr/local/share/ca-certificates/
COPY ./certs/NewExnessIssuedCA.crt /usr/local/share/ca-certificates/
COPY ./certs/NewExnessRootCA.crt /usr/local/share/ca-certificates/
COPY ./certs/combined.crt /etc/ssl/certs/ca-certificates.crt

# Import certificates to Java keystore
RUN keytool -import -trustcacerts -alias exness-ca -file /usr/local/share/ca-certificates/ExnessRootCA.crt -cacerts -storepass changeit -noprompt && \
    keytool -import -trustcacerts -alias exness-srv -file /usr/local/share/ca-certificates/ExnessServerCA.crt -cacerts -storepass changeit -noprompt && \
    keytool -import -trustcacerts -alias new-exness-ca -file /usr/local/share/ca-certificates/NewExnessRootCA.crt -cacerts -storepass changeit -noprompt && \
    keytool -import -trustcacerts -alias new-exness-issued-ca -file /usr/local/share/ca-certificates/NewExnessIssuedCA.crt -cacerts -storepass changeit -noprompt

# Remove existing plugins that we'll replace
RUN rm -rf /usr/lib/trino/plugin/postgresql/* && \
    rm -rf /usr/lib/trino/plugin/vertica/* && \
    rm -rf /usr/lib/trino/plugin/kafka/* && \
    rm -rf /usr/lib/trino/plugin/delta-lake/* && \
    rm -rf /usr/lib/trino/plugin/iceberg/* && \
    rm -rf /usr/lib/trino/lib/io.trino_trino-main-${TRINO_VERSION}.jar

# Copy built plugins from builder stage
COPY --from=builder /artifacts/plugins/postgresql /usr/lib/trino/plugin/postgresql
COPY --from=builder /artifacts/plugins/vertica /usr/lib/trino/plugin/vertica
COPY --from=builder /artifacts/plugins/kafka /usr/lib/trino/plugin/kafka
COPY --from=builder /artifacts/plugins/delta-lake /usr/lib/trino/plugin/delta-lake
COPY --from=builder /artifacts/plugins/iceberg /usr/lib/trino/plugin/iceberg
COPY --from=builder /artifacts/lib/io.trino_trino-main-${TRINO_VERSION}.jar /usr/lib/trino/lib/

# Copy Kafka configurations
RUN mkdir -p /etc/kafka
COPY kafka_table_descriptions/ /etc/kafka/

RUN mkdir -p /etc/kafka_certs
COPY kafka_certs/ /etc/kafka_certs/

# Copy storage plugin (not built from source)
RUN mkdir -p /usr/lib/trino/plugin/storage
COPY trino-storage/ /usr/lib/trino/plugin/storage/

# Set permissions for plugins
RUN chmod -R +r /usr/lib/trino/plugin/vertica && \
    chmod -R +r /usr/lib/trino/plugin/kafka && \
    chmod -R +r /usr/lib/trino/plugin/storage && \
    chmod -R +r /usr/lib/trino/plugin/delta-lake && \
    chmod -R +r /usr/lib/trino/plugin/iceberg && \
    chmod -R +r /etc/kafka && \
    chmod -R +r /etc/kafka_certs && \
    chmod -R +r /usr/lib/trino/plugin/postgresql && \
    mkdir -p /opt/java/openjdk/lib/security && \
    chmod --reference=/usr/lib/jvm/temurin/jdk-24+36/lib/security /opt/java/openjdk/lib/security && \
    ln -s /usr/lib/jvm/temurin/jdk-24+36/lib/security/cacerts /opt/java/openjdk/lib/security/cacerts

# Copy and install custom plugins from other_plugins folder
# Each JAR gets its own directory in /usr/lib/trino/plugin/
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

# Set permissions for custom plugins
RUN chmod -R a+r /usr/lib/trino/plugin/jwt \
                 /usr/lib/trino/plugin/api \
                 /usr/lib/trino/plugin/random-functions \
                 /usr/lib/trino/plugin/big-sequence \
                 /usr/lib/trino/plugin/convert-to-western-digits

# Switch back to trino user
USER trino:trino
