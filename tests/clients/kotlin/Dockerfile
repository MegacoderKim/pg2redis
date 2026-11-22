FROM gradle:8.10-jdk21

WORKDIR /app
COPY settings.gradle.kts build.gradle.kts ./
COPY src ./src
RUN gradle --no-daemon installDist

CMD ["/app/build/install/pg-redis-kotlin-test/bin/pg-redis-kotlin-test"]
