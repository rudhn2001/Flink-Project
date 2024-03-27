import org.apache.commons.lang3.ObjectUtils.Null;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsumeFlinkData {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // flag

        AtomicBoolean flag = new AtomicBoolean(false);

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



        // Load API template from JSON
        String keyField;
        List<String> fields = new ArrayList<>();
        Map<String, String> staticFields;
        try (FileInputStream fis = new FileInputStream("api_template.json")) {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(fis);
            keyField = jsonNode.get("key").asText();
            JsonNode fieldsNode = jsonNode.get("fields");
            if (fieldsNode.isArray()) {
                Iterator<JsonNode> iterator = fieldsNode.elements();
                while (iterator.hasNext()) {
                    JsonNode fieldNode = iterator.next();
                    fields.add(fieldNode.asText());
                }
            }

            JsonNode staticFieldsNode = jsonNode.get("staticFields");
            staticFields = objectMapper.convertValue(staticFieldsNode, Map.class);
        }


        // TOPICS TO PRODUCE MESSAGES INTO

        String processedTopic = "requested-data";
        String fallBackTopic = "missing-data";



        // Process data from Kafka and send to new topic
        
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
                .setValueSerializationSchema(new SimpleStringSchema())
                .build();

        KafkaSink<String> missingDataSink = KafkaSink.<String>builder()
                .setKafkaProducerConfig(producerProps)
                .setRecordSerializer(missingDataSchema)
                .build();

        // FOR REQUESTS CONTAINING CORRECT VALUES

        KafkaRecordSerializationSchema<String> processedDataSchema = KafkaRecordSerializationSchema.<String>builder()
                .setTopic(processedTopic)
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

            // Process data according to the template
            StringBuilder processedDataBuilder = new StringBuilder();

            // handle the key for message

            JsonNode keyNode = jsonNode.at(keyField);
            String key = (keyNode != null && !keyNode.isMissingNode()) ?  keyNode.asText() : "KEY";


            // Process dynamic fields

            for (String fieldPath : fields) {
                String fieldName = fieldPath.substring(fieldPath.lastIndexOf("/") + 1);
                JsonNode fieldValue = jsonNode.at(fieldPath);
                if (fieldValue != null && !fieldValue.isMissingNode()) {
                    processedDataBuilder.append(fieldName).append(": ").append(fieldValue.asText()).append("\n");
                } else {
                    // Handle missing field value (optional)
                    processedDataBuilder.append(fieldName).append(": ").append("MISSING").append("\n");
                    flag.set(true);
                }
            }
            // flag.set(allFieldPresent);



            // Process static fields
            for (Map.Entry<String, String> entry : staticFields.entrySet()) {
                processedDataBuilder.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
            }

            return processedDataBuilder.toString();

        });


        // SEND THE DATA TO THE TOPOC


        // SENDS THE DATA TO REQUESTED-DATA TOPIC(PROVIDED ALL FIELDS EXISTS)
        processedDataStream
        .filter(data -> !data.contains("MISSING"))
        .sinkTo(processedDataSink);


        // SENDS THE DATA TO MISSING-DATA TOPIC(PROVIDED ANY ONE OF FIELDS DOESN'T EXISTS)
        processedDataStream
        .filter(data -> data.contains("MISSING"))
        .sinkTo(missingDataSink);


        // Print raw data for debugging(ISSUES)
        userDataStream.print();

        env.execute();
    }
}
