package producers;

import classes.Trip;
import classes.Route;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import utils.JsonSerializer;

import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

public class TripsProducerCenario {

    private static final String TOPIC_NAME = "Trips_topic";
    private static final String BOOTSTRAP_SERVERS = "broker1:9092,broker2:9093,broker3:9094";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", JsonSerializer.class.getName());

        try (Producer<String, Trip> producer = new KafkaProducer<>(properties)) {
            Random random = new Random(5);
            Timer timer = new Timer();

            // Variável para contar os segundos
            final int[] elapsedSeconds = {0};

            // Gera 1 trip a cada 10 segundos
            timer.scheduleAtFixedRate(new TimerTask() {
                int tripCounter = 30;

                @Override
                public void run() {
                    elapsedSeconds[0] += 10;

                    if (elapsedSeconds[0] % 60 == 0) {
                        System.out.println("Passou 1 minuto");
                    }

                    List<Route> availableRoutes = RoutesProducerCenario.recentRoutes;

                    if (availableRoutes.isEmpty()) {
                        System.out.println("Nenhuma rota disponível. Aguardando...");
                        return;
                    }

                    // Escolhe uma rota aleatória
                    Route chosenRoute = availableRoutes.get(random.nextInt(availableRoutes.size()));

                    // Cria a trip com base na rota escolhida
                    Trip trip = new Trip();
                    trip.setTripId("Trip_" + tripCounter++);
                    trip.setRouteId(chosenRoute.getRouteId());
                    trip.setOrigin(chosenRoute.getOrigin());
                    trip.setDestination(chosenRoute.getDestination());
                    trip.setTransportType(chosenRoute.getTransportType());
                    trip.setPassengerName("Passenger_" + random.nextInt(1000));

                    // Envia a trip
                    ProducerRecord<String, Trip> record = new ProducerRecord<>(TOPIC_NAME, trip.getTripId(), trip);
                    producer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            System.err.println("Erro ao enviar viagem: " + exception.getMessage());
                        } else {
                            System.out.printf("Viagem enviada: %s [Route=%s, Transporte=%s, Partição=%d, Offset=%d]%n",
                                    trip.getTripId(), trip.getRouteId(), trip.getTransportType(), metadata.partition(), metadata.offset());
                        }
                    });
                }
            }, 0, 10000L); // Começa imediatamente (0 ms) e repete a cada 10 segundos

            System.out.println("Produtor de viagens iniciado. Pressione Ctrl+C para encerrar.");
            Thread.sleep(Long.MAX_VALUE);
        } catch (Exception e) {
            System.err.println("Erro ao criar o produtor Kafka: " + e.getMessage());
        }
    }
}