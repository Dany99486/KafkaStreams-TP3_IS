package producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

public class TripsProducer {

    private static final String TOPIC_NAME = "Trips_topic";
    private static final String BOOTSTRAP_SERVERS = "broker1:9092,broker2:9093,broker3:9094";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (Producer<String, String> producer = new KafkaProducer<>(properties)) {
            String[] transportTypes = {"Bus", "Taxi", "Train", "Metro", "Scooter"};
            Random random = new Random();
            Timer timer = new Timer();

            timer.scheduleAtFixedRate(new TimerTask() {
                int tripCounter = 1;

                @Override
                public void run() {
                    String tripId = "Trip_" + tripCounter++;
                    String routeId = "Route_" + random.nextInt(100);
                    String origin = "Origin_" + random.nextInt(10);
                    String destination = "Destination_" + random.nextInt(10);
                    String transportType = transportTypes[random.nextInt(transportTypes.length)];
                    String passengerName = "Passenger_" + random.nextInt(1000);


                    String schema = """
                        {
                            "type": "struct",
                            "fields": [
                                {"field": "tripId", "type": "string"},
                                {"field": "routeId", "type": "string"},
                                {"field": "origin", "type": "string"},
                                {"field": "destination", "type": "string"},
                                {"field": "transportType", "type": "string"},
                                {"field": "passengerName", "type": "string"}
                            ]
                        }
                    """;

                    //Payload
                    String payload = String.format(
                            "{ \"tripId\": \"%s\", \"routeId\": \"%s\", \"origin\": \"%s\", \"destination\": \"%s\", \"transportType\": \"%s\", \"passengerName\": \"%s\" }",
                            tripId, routeId, origin, destination, transportType, passengerName
                    );

                    //Mensagem final com schema e payload
                    String message = String.format(
                            "{ \"schema\": %s, \"payload\": %s }",
                            schema, payload
                    );


                    ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, tripId, message);
                    producer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            System.err.println("Erro ao enviar mensagem: " + exception.getMessage());
                        } else {
                            System.out.printf("Viagem enviada para o tópico %s: TripId=%s, Partição=%d, Offset=%d%n",
                                    metadata.topic(), tripId, metadata.partition(), metadata.offset());
                        }
                    });
                }
            }, 0, 5000);

            System.out.println("Produtor de viagens iniciado. Pressione Ctrl+C para encerrar.");
            Thread.sleep(Long.MAX_VALUE);
        } catch (Exception e) {
            System.err.println("Erro ao criar o produtor Kafka: " + e.getMessage());
        }
    }
}