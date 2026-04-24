FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app
COPY backend/pom.xml .
RUN mvn dependency:resolve -q
COPY backend/src ./src
RUN mvn package -DskipTests -q

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

COPY --from=build /app/target/datatrust-engine-*.jar app.jar
COPY dashboard/ ./dashboard/

ENV PORT=4000
ENV DASHBOARD_PATH=/app/dashboard
ENV DB_PATH=/app/data/datatrust.db

RUN mkdir -p /app/data /app/logs

EXPOSE 4000

ENTRYPOINT ["java", "-jar", "app.jar"]
