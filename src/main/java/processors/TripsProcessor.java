package processors;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

public class TripsProcessor {
    public static void main(String[] args) {
        // Configurações do Producer para o Passenger Trips Topic
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", "broker1:9092,broker2:9092,broker3:9092");
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        // Dados simulados
        Random random = new Random();
        String[] transportTypes = {"Bus", "Taxi", "Train", "Metro", "Scooter"};
        String[] cities = {"City A", "City B", "City C", "City D", "City E"};
        String[] passengers = {"Alice", "Bob", "Charlie", "Diana", "Edward"};

        System.out.println("Trips Processor iniciado. Gerando dados de viagens...");

        while (true) {
            // Gerar dados simulados para uma viagem
            String routeId = "R" + random.nextInt(100); // Identificador da rota (simulado)
            String origin = cities[random.nextInt(cities.length)];
            String destination = cities[random.nextInt(cities.length)];
            String transportType = transportTypes[random.nextInt(transportTypes.length)];
            String passengerName = passengers[random.nextInt(passengers.length)];

            // Criar a mensagem JSON para a viagem
            String tripValue = String.format(
                "{\"route_id\":\"%s\",\"origin\":\"%s\",\"destination\":\"%s\",\"passenger_name\":\"%s\",\"transport_type\":\"%s\"}",
                routeId, origin, destination, passengerName, transportType
            );

            // Enviar a mensagem para o Passenger Trips Topic
            producer.send(new ProducerRecord<>("Passanger_trips_topic", routeId, tripValue));
            System.out.println("Enviado para Passenger Trips_topic: " + tripValue);

            // Pausar antes de gerar a próxima viagem
            try {
                Thread.sleep(1000); // 1 segundo entre as viagens
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}