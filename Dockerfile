FROM flink:latest

copy target/events-1.0-SNAPSHOT.jar /opt/flink/usrlib/

CMD ["flink", "run", "-c", "DataGeneratorJob", "/opt/flink/usrlib/events-1.0-SNAPSHOT.jar"]

# CMD ["flink", "run", "-c", "ConsumeFlinkData.java", "/opt/flink/usrlib/events-1.0-SNAPSHOT.jar"]