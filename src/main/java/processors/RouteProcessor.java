package processors;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;

public class RouteProcessor {
    public static void main(String[] args) {
        // Configurações do Consumer (para DBInfo-routes_capacity)
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", "broker1:9092,broker2:9092,broker3:9092");
        consumerProps.put("group.id", "route-processor-group-teste");
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("auto.offset.reset", "earliest"); // Para ler do início se nunca leu antes

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList("DBInfo-routes_capacity")); // Tópico de entrada

        // Configurações do Producer (para Routes Topic)
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", "broker1:9092,broker2:9092,broker3:9092");
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);
        Random random = new Random();
        String[] transportTypes = {"Bus", "Taxi", "Train", "Metro", "Scooter"};
        String[] cities = {"City A", "City B", "City C", "City D", "City E"};
        String[] operators = {"Operator A", "Operator B", "Operator C"};

        System.out.println("Route Processor iniciado. Aguardando mensagens do tópico DBInfo-routes_capacity...");

        while (true) {
            // Consumir mensagens do DBInfo-routes_capacity
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : records) {
                // Dados consumidos
                String dbInfoValue = record.value(); // Exemplo: {"id":1,"capacity":50}

                // Parse simples (em produção, use bibliotecas como Jackson para JSON parsing)
                String capacity = dbInfoValue.split("\"capacity\":")[1].split("}")[0];

                // Simular outros dados
                String routeId = "R" + random.nextInt(100);
                String origin = cities[random.nextInt(cities.length)];
                String destination = cities[random.nextInt(cities.length)];
                String transportType = transportTypes[random.nextInt(transportTypes.length)];
                String operatorName = operators[random.nextInt(operators.length)];

                // Criar mensagem JSON para Routes Topic
                String routesValue = String.format(
                    "{\"route_id\":\"%s\",\"capacity\":%s,\"origin\":\"%s\",\"destination\":\"%s\",\"transport_type\":\"%s\",\"operator_name\":\"%s\"}",
                    routeId, capacity, origin, destination, transportType, operatorName
                );

                // Enviar mensagem para Routes Topic
                producer.send(new ProducerRecord<>("Routes_topic", routeId, routesValue));
                System.out.println("Enviado para Routes_topic: " + routesValue);
            }
        }
    }
}