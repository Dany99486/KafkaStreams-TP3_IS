package streams;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.json.JSONObject;

import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class PassengersPerRoute {

    public static void main(String[] args) {
        // Configuração para Kafka Streams
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "passengers-per-route-app1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9093,broker3:9094");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Nome do tópico de saída
        String outputTopic = "projeto3_passengers_per_route";

        // Criação do tópico de saída, se necessário
        createTopicIfNotExists(outputTopic, props);

        StreamsBuilder builder = new StreamsBuilder();

        // Consome mensagens do tópico "Trips_topic"
        KStream<String, String> tripsStream = builder.stream("Trips_topic");

        KTable<String, Long> passengersPerRoute = tripsStream
            .mapValues((String value) -> {
                try {
                    JSONObject json = new JSONObject(value);
                    JSONObject payload = json.getJSONObject("payload");
                    return payload.getString("routeId");
                } catch (Exception e) {
                    System.err.println("Erro ao parsear JSON: " + e.getMessage());
                    return null; // Retorna null se houver erro
                }
            })
            .filter((key, routeId) -> routeId != null)
            .groupBy((key, routeId) -> routeId)
            .count();

        passengersPerRoute.toStream()
            .mapValues((routeId, count) -> {
                String schema = """
                    {
                        "type": "struct",
                        "fields": [
                            {"field": "routeId", "type": "string"},
                            {"field": "passengerCount", "type": "int64"}
                        ]
                    }
                """;

                String payload = String.format(
                    "{\"routeId\": \"%s\", \"passengerCount\": %d}",
                    routeId, count
                );

                return String.format("{\"schema\": %s, \"payload\": %s}", schema, payload);
            })
            .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static void createTopicIfNotExists(String topicName, Properties props) {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, props.getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG));
        try (Admin adminClient = Admin.create(adminProps)) {
            Set<String> existingTopics = adminClient.listTopics().names().get();
            if (!existingTopics.contains(topicName)) {
                NewTopic newTopic = new NewTopic(topicName, 3, (short) 3); // 3 partitions, replication factor of 3
                adminClient.createTopics(Collections.singleton(newTopic)).all().get();
                System.out.printf("Tópico '%s' criado com sucesso!%n", topicName);
            } else {
                System.out.printf("Tópico '%s' já existe.%n", topicName);
            }
        } catch (ExecutionException | InterruptedException e) {
            System.err.printf("Erro ao criar/verificar o tópico '%s': %s%n", topicName, e.getMessage());
        }
    }
}