FROM eclipse-temurin:17-jdk
RUN apt update && \
    apt -y --no-install-recommends --no-install-suggests install libjemalloc2 curl && \
    apt clean && rm -rf /var/lib/apt/lists/*
ENV LD_PRELOAD=/usr/lib/aarch64-linux-gnu/libjemalloc.so.2
EXPOSE 8080

ARG JAR_FILE=ke-analytics-0.0.1.jar
ARG CERT_DIR=classes/cert
COPY ${JAR_FILE} /opt/ke-analytics/ke-analytics.jar
COPY ${CERT_DIR} /opt/ke-analytics/cert

ENTRYPOINT ["java", "-server", "-Xms256M", "-Xss256k",\
            "-XX:+UnlockDiagnosticVMOptions", "-XX:+UseContainerSupport",\
            "-XX:+HeapDumpOnOutOfMemoryError", "-XX:HeapDumpPath=dump.hprof", "-Djava.security.egd=/dev/zrandom",\
            "-XX:+UseParallelGC", "-XX:MinHeapFreeRatio=5", "-XX:MaxHeapFreeRatio=10", "-XX:GCTimeRatio=4", "-XX:AdaptiveSizePolicyWeight=90",\
            "-XX:MaxRAMFraction=2",\
            "-Djavax.net.ssl.trustStore=/opt/ke-analytics/cert/marketdb.jks", "-Djavax.net.ssl.trustStorePassword=StrongPasswd",\
            "-jar", "/opt/ke-analytics/ke-analytics.jar"]

WORKDIR /opt/ke-analytics
