// FOR NORMAL FLINK SOURCE AND SINK

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// PARSING THE JSON
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;


import org.apache.flink.table.api.*;
import org.apache.flink.types.Row;
import scala.collection.mutable.StringBuilder;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import java.util.Properties;
import java.util.Scanner;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class ConsumeFlinkData {

    public static void main(String[] args) throws Exception {

        // for template variables

        String keyField;
        List<String> fields = new ArrayList<>();

        // TOPICS TO PRODUCE MESSAGES INTO

        String processedTopic = "requested-data";
        String fallBackTopic = "missing-data";

        // PRODUCER PROPS

        Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers", "localhost:9092");
        producerProps.setProperty("acks", "1");
        producerProps.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.setProperty("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        // GET CONSUMER PROPS FROM RESOURCES/CONSUME.PROPS

        Properties consumerProps = new Properties();
        try (InputStream stream = ConsumeFlinkData.class.getClassLoader().getResourceAsStream("consumer.properties")) {
            consumerProps.load(stream);
        }

        // ASK THE VALUES

        Scanner input = new Scanner(System.in);
        System.out.println("Enter the Key template : ");
        String Key = input.nextLine();
        System.out.println("Enter the Value template : ");
        String Value = input.nextLine();
        // System.out.println("Enter the event processor in JSON : ");
        // String matcherString = input.nextLine();

        // PROCESS KEY TEMPLATE
        Pattern keyPattern = Pattern.compile("\\{\\{(.*?)\\}\\}");
        Matcher keyMatcher = keyPattern.matcher(Key);

        if (keyMatcher.find()) {
            String keyPath = keyMatcher.group(1).replace(".", "/");
            keyField = "/" + keyPath;
        } else {
            throw new IllegalArgumentException("Invalid key template format.");
        }

        // PROCESS VALUE TEMPLATE

        Pattern valuePattern = Pattern.compile("\\{\\{(.*?)\\}\\}");
        Matcher valueMatcher = valuePattern.matcher(Value);
        while (valueMatcher.find()) {
            String valuePath = valueMatcher.group(1).replace(".", "/");
            String valueField = "/" + valuePath;
            fields.add(valueField);
        }

//        Pattern staticPattern = Pattern.compile("\"(.*?)\".");
//        Matcher staticValueMatcher = staticPattern.matcher(Value);
//        while(staticValueMatcher.find()){
//            String staticField = staticValueMatcher.group(1);
//            fields.add(staticField);
//        }

        // ------- CREATE STREAM ENVIRONMENT AND TABLE SOURCE -------------

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);



        // ------------- KAFKA SOURCE BUILD ----------------

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setProperties(consumerProps)
                .setTopics("userdata")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // ------------- MAKE A KAFKA DATASTREAM ----------------
        DataStream<String> userDataStream = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "userdata_source");




        // FOR REQUESTS CONTAINING MISSING VALUES
        KafkaRecordSerializationSchema<String> missingDataSchema = KafkaRecordSerializationSchema
                .<String>builder()
                .setTopic(fallBackTopic)
                // .setKeySerializationSchema(new SimpleStringSchema())
                .setValueSerializationSchema(new SimpleStringSchema())
                .build();

        KafkaSink<String> missingDataSink = KafkaSink.<String>builder()
                .setKafkaProducerConfig(producerProps)
                .setRecordSerializer(missingDataSchema)
                .build();

        // FOR REQUESTS CONTAINING CORRECT VALUES
        KafkaRecordSerializationSchema<String> processedDataSchema = KafkaRecordSerializationSchema.<String>builder()
                .setTopic(processedTopic)
                // .setKeySerializationSchema(new SimpleStringSchema())
                .setValueSerializationSchema(new SimpleStringSchema())
                .build();

        KafkaSink<String> processedDataSink = KafkaSink.<String>builder()
                .setKafkaProducerConfig(producerProps)
                .setRecordSerializer(processedDataSchema)
                .build();

        // FOR PROCESSING DATA, TAKES RAW DATA AND FILTERS USING THE TEMPLATES

        // READ AND PARSE MATCHERS.JSON
        GetMatchers matchers = new GetMatchers();
//        IF READING FROM INPUT
        System.out.println("Enter the Event Processor template : ");
        String Matchers = input.nextLine();
        matchers.CheckCondition(Matchers);

//        IF READING FROM FILE
//        try (FileInputStream fis = new FileInputStream("matcher.json")) {
//            String jsonString = new String(fis.readAllBytes());
//            matchers.CheckCondition(jsonString);
//        } catch (Exception e) {
//            System.out.println("\n \t" + e);
//        }

        // ------------- CONVERT STREAM TO TABLE AND ADD INTO NEW ROW ----------------

        Table inputTable = tenv.fromDataStream(userDataStream);
        tenv.createTemporaryView("InputTable", inputTable);
        // ------------- QUERY ----------------
        StringBuilder queryBuilder = new StringBuilder("SELECT * FROM InputTable WHERE ");
        for (int i = 0; i < matchers.getFields().size(); i++) {
            String field = matchers.getFields().get(i);
            String operator = matchers.getOperators().get(i);
            String value = matchers.getValues().get(i);
            String combinator = matchers.getCombinator();

            queryBuilder.append("JSON_VALUE(f0, \'$.").append(field + "\') ").append(operator+" ").append("\'"+value+"\'");
                if (i < matchers.getFields().size() - 1) {
                    queryBuilder.append(" "+combinator+" ");
                }
        }
        System.out.println("SQL QUERY : " + queryBuilder);
        // -------------RUN THE QUERY AND THEN CONVERT TABLESTREAM TO DATASTREAM AGAIN ----------------
        Table resultTable = tenv.sqlQuery(queryBuilder.toString());

        DataStream<Row> resultStream = tenv.toDataStream(resultTable);

        DataStream<String> processedDataStream = resultStream
                .map(row -> row.toString().substring(3))
                .map(rawData -> {


            // Parse raw data as JSON
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(String.valueOf(rawData));

            // flag
            int flag = 0;

            JsonNode keyValue = jsonNode.at(keyField);

            // Process data according to the template
            StringBuilder processedDataBuilder = new StringBuilder();
            int keyStartIndex = processedDataBuilder.length();
            String keyName = keyField.substring(keyField.lastIndexOf("/") + 1);
            if (keyValue != null && !keyValue.isMissingNode() && flag == 0) {
                processedDataBuilder.append("Key : ").append(keyValue).append("\nValues : {");
            } else {
                processedDataBuilder.append("\"" + "ERROR" + " \" ").append(": \"" + keyName + " MISSING" + " \" \n")
                        .append(rawData);
                flag = 1;
            }
            // Process dynamic fields
            if (flag == 0) {
                for (String fieldPath : fields) {
                    String fieldName = fieldPath.substring(fieldPath.lastIndexOf("/") + 1);
                    JsonNode fieldValue = jsonNode.at(fieldPath);
                    if (fieldValue != null && !fieldValue.isMissingNode() && flag == 0) {
                        if (fieldValue.isObject()) {
                            processedDataBuilder.append(" \"" + fieldName + " \"").append(": ")
                                    .append(fieldValue.toString()); // Append JSON structure as is
                        } else {
                            processedDataBuilder.append(" \"" + fieldName + " \"").append(": ")
                                    .append(" \"" + fieldValue.asText() + " \"");
                        }
                    } else {
                        processedDataBuilder.delete(keyStartIndex, processedDataBuilder.length());
                        // Handle missing field value
                        processedDataBuilder.append("\"" + "ERROR" + " \"")
                                .append(": \"" + fieldName + " MISSING" + " \" \n")
                                .append(rawData);
                        flag = 1;
                    }

                }
            }
            processedDataBuilder.append("}");
            return processedDataBuilder.toString();

        });


//         SEND THE DATA TO THE TOPIC
//         SENDS THE DATA TO REQUESTED-DATA TOPIC(PROVIDED ALL FIELDS EXISTS)
        processedDataStream
                .filter(data -> !data.contains("MISSING"))
                .sinkTo(processedDataSink);

        // SENDS THE DATA TO MISSING-DATA TOPIC(PROVIDED ANY ONE OF FIELDS DOESN'T
        // EXISTS)
        processedDataStream
                .filter(data -> data.contains("MISSING"))
                .sinkTo(missingDataSink);


        // ------------- PRINT IN LOG LIST ----------------
        resultStream.print();
//        userDataStream.print();

        env.execute();
    }
}

