FROM maven:3.8.4-jdk-11 AS builder

WORKDIR /usr/local/flink-job

COPY pom.xml .
COPY src/ ./src/

RUN mvn clean package

FROM flink:latest
#working directory in the Flink container

WORKDIR /usr/local/flink-job

COPY --from=builder /usr/local/flink-job/ .
# Set the default command to run the Flink job
