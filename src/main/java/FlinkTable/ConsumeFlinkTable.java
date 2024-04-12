package FlinkTable;
// FOR NORMAL FLINK SOURCE AND SINK


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;



import org.apache.flink.table.api.*;
import org.apache.flink.types.Row;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.apache.flink.table.api.Table;

import java.io.InputStream;
import java.util.Properties;


public class ConsumeFlinkTable {
    public static void main(String[] args) throws Exception{
        // create environments of both APIs
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Properties consumerProps = new Properties();
        try (InputStream stream = ConsumeFlinkTable.class.getClassLoader().getResourceAsStream("consumer.properties")) {
            consumerProps.load(stream);
        }
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setProperties(consumerProps)
                .setTopics("userdata")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        DataStream<String> userDataStream = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "userdata_source");

//
//// create a DataStream
//        DataStream<String> dataStream = env.fromElements("Alice", "Bob", "John");

// interpret the insert-only DataStream as a Table
        Table inputTable = tableEnv.fromDataStream(userDataStream);

// register the Table object as a view and query it
        tableEnv.createTemporaryView("InputTable", inputTable);
        TableSchema schema = inputTable.getSchema();
        String[] fieldNames = schema.getFieldNames();
        System.out.println("Schema of inputTable:");
        for (String fieldName : fieldNames) {
            System.out.println(fieldName);
        }
        Table resultTable = tableEnv.sqlQuery("SELECT * FROM InputTable WHERE JSON_VALUE(f0, '$.response.status') = '200'");

//        StringBuilder queryBuilder = new StringBuilder("SELECT * FROM `userdata` WHERE ");
//        for (int i = 0; i < matchers.getFields().size(); i++) {
//            String field = matchers.getFields().get(i);
//            String operator = matchers.getOperators().get(i);
//            String value = matchers.getValues().get(i);
//            queryBuilder.append("`").append(field).append("`").append(" ").append(operator).append(" '").append(value).append("'");
//
//            if (i < matchers.getFields().size() - 1) {
//                queryBuilder.append(" AND");
//            }
//        }
//        queryBuilder.append(";");


// interpret the insert-only Table as a DataStream again
        DataStream<Row> resultStream = tableEnv.toDataStream(resultTable);

// add a printing sink and execute in DataStream API
        resultStream.map(row -> row.toString().substring(3)).print();
        env.execute();

// prints:
// +I[ALICE]
// +I[BOB]
// +I[JOHN]
    }
}
