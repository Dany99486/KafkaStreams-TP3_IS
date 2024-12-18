package producers;

import classes.Route;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import utils.JsonSerializer;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

public class RoutesProducerCenario {

    private static final String TOPIC_NAME = "Routes_topic";
    private static final String BOOTSTRAP_SERVERS = "broker1:9092,broker2:9093,broker3:9094";
    private static final String[] TRANSPORT_TYPES = {"Bus", "Taxi", "Train", "Metro", "Scooter"};
    public static List<Route> recentRoutes = new CopyOnWriteArrayList<>();

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", JsonSerializer.class.getName());

        try (Producer<String, Route> producer = new KafkaProducer<>(properties)) {
            Random random = new Random(5);
            Timer timer = new Timer();

            // A cada 1 minuto, cria 5 rotas e pausa
            timer.scheduleAtFixedRate(new TimerTask() {
                int routeCounter = 1;

                @Override
                public void run() {
                    System.out.println("=== Início da criação de 5 rotas ===");

                    for (int i = 0; i < 5; i++) {
                        Route route = new Route();
                        String routeId = "Route_" + routeCounter++;
                        route.setRouteId(routeId);
                        route.setOrigin("Origin_" + random.nextInt(10));
                        route.setDestination("Destination_" + random.nextInt(10));
                        String chosenTransport = TRANSPORT_TYPES[random.nextInt(TRANSPORT_TYPES.length)];
                        route.setTransportType(chosenTransport);
                        route.setCapacity(random.nextInt(200) + 50);
                        route.setOperator("Operator_" + random.nextInt(5));

                        recentRoutes.add(route);
                        if (recentRoutes.size() > 100) {
                            recentRoutes.remove(0);
                        }

                        ProducerRecord<String, Route> record = new ProducerRecord<>(TOPIC_NAME, route.getRouteId(), route);
                        producer.send(record, (metadata, exception) -> {
                            if (exception != null) {
                                System.err.println("Erro ao enviar rota: " + exception.getMessage());
                            } else {
                                System.out.printf("Rota enviada: %s [Transporte: %s, Partição=%d, Offset=%d]%n",
                                        route.getRouteId(), route.getTransportType(), metadata.partition(), metadata.offset());
                            }
                        });
                    }

                    System.out.println("=== Fim da criação de 5 rotas ===\n");
                }
            }, 0, 2 * 60000L); // Executa a cada 2 minutos para garantir 1 minuto de pausa após 5 rotas.

            System.out.println("Produtor de rotas iniciado. Pressione Ctrl+C para encerrar.");
            Thread.sleep(Long.MAX_VALUE);
        } catch (Exception e) {
            System.err.println("Erro ao criar o produtor Kafka: " + e.getMessage());
        }
    }
}