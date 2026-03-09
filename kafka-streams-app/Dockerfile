FROM maven:3.9-eclipse-temurin-17 AS builder
WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline -B
COPY src ./src
RUN mvn package -DskipTests -B

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=builder /app/target/anomaly-detector-1.0.0.jar app.jar

ENV KAFKA_BOOTSTRAP_SERVERS=localhost:9092
ENV APPLICATION_ID=o2-anomaly-detector
ENV DYNAMODB_TABLE=o2-platform-feature-store
ENV DYNAMODB_REGION=us-east-1
ENV INPUT_TOPICS=transactions,claims,logins,transfers
ENV OUTPUT_TOPIC=anomalies

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "app.jar"]
