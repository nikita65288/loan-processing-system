# Each service is packaged by the Maven reactor before building its image.
FROM eclipse-temurin:17.0.17_10-jre-jammy
WORKDIR /app
RUN groupadd -g 10001 lps && useradd --uid 10001 --gid 10001 --home-dir /app --no-create-home lps \
    && mkdir -p /app/logs && chown -R 10001:10001 /app
ARG JAR_FILE
COPY --chown=10001:10001 ${JAR_FILE} /app/app.jar
USER 10001:10001
ENV JDK_JAVA_OPTIONS="-XX:MaxRAMPercentage=55.0 -XX:+ExitOnOutOfMemoryError"
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
