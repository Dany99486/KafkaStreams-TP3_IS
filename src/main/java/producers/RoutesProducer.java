package producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

public class RoutesProducer {

    private static final String TOPIC_NAME = "Routes_topic";
    private static final String BOOTSTRAP_SERVERS = "broker1:9092,broker2:9093,broker3:9094";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (Producer<String, String> producer = new KafkaProducer<>(properties)) {
            String[] suppliers = {"Supplier A", "Supplier B", "Supplier C", "Supplier D",
                                  "Supplier E", "Supplier F", "Supplier G", "Supplier H",
                                  "Supplier I", "Supplier J"};
            String[] transportTypes = {"Bus", "Taxi", "Train", "Metro", "Scooter"};
            Random random = new Random();
            Timer timer = new Timer();

            timer.scheduleAtFixedRate(new TimerTask() {
                int routeCounter = 1;

                @Override
                public void run() {
                    String routeId = "Route_" + routeCounter++;
                    String origin = "Origin_" + random.nextInt(10);
                    String destination = "Destination_" + random.nextInt(10);
                    String transportType = transportTypes[random.nextInt(transportTypes.length)];
                    int capacity = random.nextInt(200) + 1;
                    String operator = suppliers[random.nextInt(suppliers.length)];

                    // Construindo o schema manualmente
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
                            ]
                        }
                    """;

                    // Construindo o payload manualmente
                    String payload = String.format(
                            "{ \"routeId\": \"%s\", \"origin\": \"%s\", \"destination\": \"%s\", \"transportType\": \"%s\", \"capacity\": %d, \"operator\": \"%s\" }",
                            routeId, origin, destination, transportType, capacity, operator
                    );

                    // Mensagem final com schema e payload
                    String message = String.format(
                            "{ \"schema\": %s, \"payload\": %s }",
                            schema, payload
                    );

                    // Enviando a mensagem
                    ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, routeId, message);
                    producer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            System.err.println("Erro ao enviar mensagem: " + exception.getMessage());
                        } else {
                            System.out.printf("Rota enviada para o tópico %s: RouteId=%s, Partição=%d, Offset=%d%n",
                                    metadata.topic(), routeId, metadata.partition(), metadata.offset());
                        }
                    });
                }
            }, 0, 5000); // 0 delay inicial, 10 segundos de intervalo

            System.out.println("Produtor de rotas iniciado. Pressione Ctrl+C para encerrar.");
            Thread.sleep(Long.MAX_VALUE); // Manter o programa rodando
        } catch (Exception e) {
            System.err.println("Erro ao criar o produtor Kafka: " + e.getMessage());
        }
    }
}