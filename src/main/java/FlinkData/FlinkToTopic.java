package FlinkData;
// import org.apache.flink.api.common.eventtime.WatermarkStrategy;
// import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
// import org.apache.flink.connector.kafka.sink.KafkaSink;   
// import org.apache.flink.formats.json.JsonSerializationSchema;
// import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
// import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
// import org.apache.flink.connector.kafka.sink.KafkaSink;
// import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
// import org.apache.flink.streaming.api.datastream.DataStream;
// import


// import java.util.Properties;


// public class FlinkToTopic {

//     public static void main(String[] args) throws Exception {
//         Properties producerProps = new Properties();
//         producerProps.setProperty("bootstrap.servers", "localhost:9092");
//         producerProps.setProperty("acks", "1");
//         producerProps.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//         producerProps.setProperty("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

//         String topicName = "requested-data";

//         KafkaRecordSerializationSchema<FlinkToTopic> userDataSchema = KafkaRecordSerializationSchema.<FlinkToTopic>builder()
//                 .setTopic(topicName)
//                 .setValueSerializationSchema(new JsonSerializationSchema<>())
//                 .build();

//         KafkaSink<FlinkToTopic> kafkaSink = KafkaSink.<FlinkToTopic>builder()
//                 .setKafkaProducerConfig(producerProps)
//                 .setRecordSerializer(userDataSchema)
//                 .build();

//         DataStream<GenerateData> userdataStream = env
//                 .fromSource(kafkaSink, WatermarkStrategy.noWatermarks(), "userdata_source");


//         StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


//         env.execute("UserData Importer Job");
//     }

//     private static ObjectMapper createObjectMapper() {
//         ObjectMapper objectMapper = new ObjectMapper();
//         objectMapper.registerModule(new JavaTimeModule());
//         return objectMapper;
//     }


// }
