package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.Reader;
import java.nio.file.Files;
import java.util.Map;
import java.util.Properties;

public class CsvToKafkaProducer {

    private static final String TOPIC = "mock-data1";
    private static final int LOG_EVERY = 1000;

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("acks", "all");
        props.put("linger.ms", "5");
        props.put("batch.size", 16384);

        ObjectMapper mapper = new ObjectMapper();

        File folder = new File("/app_kafka/data");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

            int totalCount = 0;

            for (File file : folder.listFiles()) {
                System.out.println("Processing file: " + file.getName());

                try (Reader reader = Files.newBufferedReader(file.toPath());
                     CSVParser csvParser = CSVFormat.DEFAULT
                             .withFirstRecordAsHeader()
                             .parse(reader)) {

                    int fileCount = 0;
                    for (CSVRecord record : csvParser) {
                        Map<String, String> jsonMap = record.toMap();
                        String json = mapper.writeValueAsString(jsonMap);
                        producer.send(new ProducerRecord<>(TOPIC, json));

                        fileCount++;
                        totalCount++;

                        if (fileCount % LOG_EVERY == 0) {
                            System.out.println("Processed " + fileCount + " rows in file " + file.getName());
                        }
                    }

                    System.out.println("Finished file: " + file.getName() + ", total rows: " + fileCount);
                }
            }

            producer.flush();
            System.out.println("All files processed. Total rows sent to Kafka: " + totalCount);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
