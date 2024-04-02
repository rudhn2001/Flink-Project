import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.LinkedOptionalMap.KeyValue;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Iterator;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConsumeFlinkData {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // flag
        // AtomicBoolean flag = new AtomicBoolean(false);

        // GET CONSUMER PROPS FROM RESOURCES/CONSUME.PROPS
        Properties consumerProps = new Properties();
        try (InputStream stream = ConsumeFlinkData.class.getClassLoader().getResourceAsStream("consumer.properties")) {
            consumerProps.load(stream);
        }

        // Kafka source build
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setProperties(consumerProps)
                .setTopics("userdata")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        DataStream<String> userDataStream = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "userdata_source");

        // // for file based template
        // String keyField;
        // List<String> fields = new ArrayList<>();
        // Map<String, String> staticFields;
        // try (FileInputStream fis = new FileInputStream("api_template.json")) {
        // ObjectMapper objectMapper = new ObjectMapper();
        // JsonNode jsonNode = objectMapper.readTree(fis);
        // keyField = jsonNode.get("key").asText();
        // JsonNode fieldsNode = jsonNode.get("fields");
        // if (fieldsNode.isArray()) {
        // Iterator<JsonNode> iterator = fieldsNode.elements();
        // while (iterator.hasNext()) {
        // JsonNode fieldNode = iterator.next();
        // fields.add(fieldNode.asText());
        // }
        // }

        // JsonNode staticFieldsNode = jsonNode.get("staticFields");
        // staticFields = objectMapper.convertValue(staticFieldsNode, Map.class);
        // }

        // ASK THE VALUES
        String keyField;
        Scanner input = new Scanner(System.in);
        System.out.println("Enter the Key template: ");
        String Key = input.nextLine();
        System.out.println("Enter the Value template: ");
        String Value = input.nextLine();

        // PROCESS KEY TEMPLATE
        Pattern keyPattern = Pattern.compile("\\{\\{(.*?)\\}\\}");
        Matcher keyMatcher = keyPattern.matcher(Key);

        if (keyMatcher.find()) {
            keyField = keyMatcher.group(1);
        } else {
            throw new IllegalArgumentException("Invalid key template format.");
        }

        // PROCESS VALUE TEMPLATES
        List<String> fields = new ArrayList<>();
        Pattern valuePattern = Pattern.compile("\\{\\{(.*?)\\}\\}");
        Matcher valueMatcher = valuePattern.matcher(Value);
        while (valueMatcher.find()) {
            String fieldPath = valueMatcher.group(1);
            fields.add(fieldPath);
        }

        // System.out.println(keyField);
        // for(String fieldPath : fields){
        // System.out.println(fieldPath);
        // }

        // TOPICS TO PRODUCE MESSAGES INTO
        String processedTopic = "requested-data";
        String fallBackTopic = "missing-data";

        // PRODUCER PROPS
        Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers", "localhost:9092");
        producerProps.setProperty("acks", "1");
        producerProps.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.setProperty("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

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
        DataStream<String> processedDataStream = userDataStream.map(rawData -> {
            // Parse raw data as JSON
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(rawData);

            int flag = 0;

            JsonNode keyValue = jsonNode.at(keyField);

            // Process data according to the template
            StringBuilder processedDataBuilder = new StringBuilder();
            int keyStartIndex = processedDataBuilder.length();
            String keyName = keyField.substring(keyField.lastIndexOf("/") + 1);
            if (keyValue != null && !keyValue.isMissingNode() && flag == 0) {
                processedDataBuilder.append("Key : ").append(keyValue).append("\nValues : {");
            } else {
                processedDataBuilder.append("\"" + "ERROR" + " \" ").append(": \"" +keyName+ " MISSING" + " \" \n")
                        .append(rawData);
                flag = 1;
            }
            // Process dynamic fields
            if (flag == 0) {
                for (String fieldPath : fields) {
                    String fieldName = fieldPath.substring(fieldPath.lastIndexOf("/") + 1);
                    JsonNode fieldValue = jsonNode.at(fieldPath);
                    if (fieldValue != null && !fieldValue.isMissingNode() && flag == 0) {
                        processedDataBuilder.append(" \"" + fieldName + " \"").append(": ")
                                .append(" \"" + fieldValue.asText() + " \"");
                    } else {
                        processedDataBuilder.delete(keyStartIndex, processedDataBuilder.length());  
                        // Handle missing field value
                        processedDataBuilder.append("\"" + "ERROR" + " \"").append(": \"" +fieldName+ " MISSING" + " \" \n")
                                .append(rawData);
                        flag = 1;
                    }

                }
            }
            processedDataBuilder.append("}");
            return processedDataBuilder.toString();

        });

        // SEND THE DATA TO THE TOPIC

        // SENDS THE DATA TO REQUESTED-DATA TOPIC(PROVIDED ALL FIELDS EXISTS)
        processedDataStream
                .filter(data -> !data.contains("MISSING"))
                .sinkTo(processedDataSink);

        // SENDS THE DATA TO MISSING-DATA TOPIC(PROVIDED ANY ONE OF FIELDS DOESN'T
        // EXISTS)
        processedDataStream
                .filter(data -> data.contains("MISSING"))
                .sinkTo(missingDataSink);

        // Print raw data for debugging(ISSUES)
        userDataStream.print();

        env.execute();
    }
}
