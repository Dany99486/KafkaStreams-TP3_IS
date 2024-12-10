package producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import classes.Trip;
import utils.JsonSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

public class TripsProducer {

    private static final String TOPIC_NAME = "Trips_topic";
    private static final String BOOTSTRAP_SERVERS = "broker1:9092,broker2:9093,broker3:9094";

    public static void main(String[] args) {
        // Configuração do produtor Kafka
        Properties properties = new Properties();
        properties.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", JsonSerializer.class.getName()); //Usar JsonSerializer

        try (Producer<String, Trip> producer = new KafkaProducer<>(properties)) {
            String[] transportTypes = {"Bus", "Taxi", "Train", "Metro", "Scooter"};
            Random random = new Random(5);
            Timer timer = new Timer();

            timer.scheduleAtFixedRate(new TimerTask() {
                int tripCounter = 1;

                @Override
                public void run() {
                    //Criar o objeto Trip
                    Trip trip = new Trip();
                    trip.setTripId("Trip_" + tripCounter++);
                    trip.setRouteId("Route_" + random.nextInt(100));
                    trip.setOrigin("Origin_" + random.nextInt(10));
                    trip.setDestination("Destination_" + random.nextInt(10));
                    trip.setTransportType(transportTypes[random.nextInt(transportTypes.length)]);
                    trip.setPassengerName("Passenger_" + random.nextInt(1000));

                    //Enviar a mensagem como objeto serializado
                    ProducerRecord<String, Trip> record = new ProducerRecord<>(TOPIC_NAME, trip.getTripId(), trip);
                    producer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            System.err.println("Erro ao enviar mensagem: " + exception.getMessage());
                        } else {
                            System.out.printf("Viagem enviada para o tópico %s: TripId=%s, RouteId=%s Partição=%d, Offset=%d%n",
                                    metadata.topic(), trip.getTripId(), trip.getRouteId(), metadata.partition(), metadata.offset());
                        }
                    });
                }
            }, 0, 5000); // 0 delay inicial, 5 segundos de intervalo

            System.out.println("Produtor de viagens iniciado. Pressione Ctrl+C para encerrar.");
            Thread.sleep(Long.MAX_VALUE);
        } catch (Exception e) {
            System.err.println("Erro ao criar o produtor Kafka: " + e.getMessage());
        }
    }
}