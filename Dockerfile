FROM eclipse-temurin:17-jre-jammy
RUN apt update && \
    apt -y --no-install-recommends --no-install-suggests install libjemalloc2 curl && \
    apt clean && rm -rf /var/lib/apt/lists/*
ENV LD_PRELOAD=/usr/lib/aarch64-linux-gnu/libjemalloc.so.2
EXPOSE 8080

WORKDIR root/
ARG JAR_FILE=target/ke-analytics-*.jar
ARG CERT_DIR=target/classes/cert
COPY ${JAR_FILE} ./app.jar
COPY ${CERT_DIR} ./cert

ENTRYPOINT ["java", "-server", "-Xms256M", "-Xss256k",\
            "-XX:+UnlockDiagnosticVMOptions", "-XX:+UseContainerSupport",\
            "-XX:+HeapDumpOnOutOfMemoryError", "-XX:HeapDumpPath=dump.hprof", "-Djava.security.egd=/dev/zrandom",\
            "-Djavax.net.ssl.trustStore=/root/cert/marketdb.jks", "-Djavax.net.ssl.trustStorePassword=StrongPasswd",\
            "-jar", "/root/app.jar"]
