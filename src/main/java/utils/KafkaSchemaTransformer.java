package utils;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaSchemaTransformer {

    private static final String INPUT_TOPIC = "Routes_topic";
    private static final String OUTPUT_TOPIC = "Routes_topic_transformed";
    private static final String BOOTSTRAP_SERVERS = "broker1:9092,broker2:9093,broker3:9094";

    public static void main(String[] args) {
        // Configuração do consumidor
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        consumerProps.put("group.id", "schema-transformer-group");
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("auto.offset.reset", "earliest");

        // Configuração do produtor
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
             KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {

            consumer.subscribe(Collections.singleton(INPUT_TOPIC));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        String schema = """
                            {
                              "type": "struct",
                              "fields": [
                                {"field": "routeId", "type": "string"},
                                {"field": "origin", "type": "string"},
                                {"field": "destination", "type": "string"},
                                {"field": "transportType", "type": "string"},
                                {"field": "capacity", "type": "int32"},
                                {"field": "operator", "type": "string"}
                              ],
                              "optional": false,
                              "name": "Route"
                            }
                        """;

                        String transformedValue = String.format("""
                            {
                              "schema": %s,
                              "payload": %s
                            }
                        """, schema, record.value());

                        ProducerRecord<String, String> transformedRecord = new ProducerRecord<>(
                                OUTPUT_TOPIC, record.key(), transformedValue);
                        producer.send(transformedRecord, (metadata, exception) -> {
                            if (exception != null) {
                                System.err.printf("Erro ao enviar mensagem transformada: %s%n", exception.getMessage());
                            } else {
                                System.out.printf("Mensagem transformada enviada para %s, partição %d, offset %d%n",
                                        metadata.topic(), metadata.partition(), metadata.offset());
                            }
                        });
                    } catch (Exception e) {
                        System.err.printf("Erro ao transformar a mensagem: %s%n", e.getMessage());
                    }
                }
            }
        }
    }
}