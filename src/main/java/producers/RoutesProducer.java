package producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import classes.Route;
import utils.JsonSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

public class RoutesProducer {

    private static final String TOPIC_NAME = "Routes_topic";
    private static final String BOOTSTRAP_SERVERS = "broker1:9092,broker2:9093,broker3:9094";

    public static void main(String[] args) {
        //Configuração do produtor Kafka
        Properties properties = new Properties();
        properties.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", JsonSerializer.class.getName()); //Usar JsonSerializer

        try (Producer<String, Route> producer = new KafkaProducer<>(properties)) {
            String[] suppliers = {"Supplier A", "Supplier B", "Supplier C", "Supplier D",
                                  "Supplier E", "Supplier F", "Supplier G", "Supplier H",
                                  "Supplier I", "Supplier J"};
            String[] transportTypes = {"Bus", "Taxi", "Train", "Metro", "Scooter"};
            Random random = new Random(5);
            Timer timer = new Timer();

            timer.scheduleAtFixedRate(new TimerTask() {
                int routeCounter = 1;

                @Override
                public void run() {
                    //Criar objeto Route
                    Route route = new Route();
                    route.setRouteId("Route_" + routeCounter++);
                    route.setOrigin("Origin_" + random.nextInt(10));
                    route.setDestination("Destination_" + random.nextInt(10));
                    route.setTransportType(transportTypes[random.nextInt(transportTypes.length)]);
                    route.setCapacity(random.nextInt(200) + 1);
                    route.setOperator(suppliers[random.nextInt(suppliers.length)]);

                    //Enviar mensagem como objeto serializado
                    ProducerRecord<String, Route> record = new ProducerRecord<>(TOPIC_NAME, route.getRouteId(), route);
                    producer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            System.err.println("Erro ao enviar mensagem: " + exception.getMessage());
                        } else {
                            System.out.printf("Rota enviada para o tópico %s: RouteId=%s, Partição=%d, Offset=%d%n",
                                    metadata.topic(), route.getRouteId(), metadata.partition(), metadata.offset());
                        }
                    });
                }
            }, 0, 5000);

            System.out.println("Produtor de rotas iniciado. Pressione Ctrl+C para encerrar.");
            Thread.sleep(Long.MAX_VALUE);
        } catch (Exception e) {
            System.err.println("Erro ao criar o produtor Kafka: " + e.getMessage());
        }
    }
}