package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkStarSchemaJob {

    public static void main(String[] args) throws Exception {
        KafkaSource<MockDataEvent> kafkaSource =
        KafkaSource.<MockDataEvent>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("mock-data1")
                .setGroupId("flink-dds1")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(
                        new JsonDeserializationSchema<>(MockDataEvent.class)
                )
                .build();
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<MockDataEvent> stream =
                env.fromSource(
                        kafkaSource,
                        WatermarkStrategy.noWatermarks(),
                        "kafka-mock-data1"
                );
         stream.addSink(new MockDataJdbcSink());
         stream.addSink(new CustomerDdsSink());
         stream.addSink(new ProductDdsSink());
         stream.addSink(new SaleDdsSink());

        env.execute("Kafka → Flink → PostgreSQL (Star Schema)");
    }
}
