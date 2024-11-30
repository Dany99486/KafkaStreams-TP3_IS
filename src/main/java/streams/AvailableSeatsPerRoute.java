package streams;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.json.JSONObject;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class AvailableSeatsPerRoute {

    private static final String OUTPUT_TOPIC = "projeto3_available_seats_per_route";

    public static void main(String[] args) {
        // Configuração do Kafka Streams
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "available-seats-per-route-app2");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9093,broker3:9094");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Verifica e cria o tópico de saída, se necessário
        createTopicIfNotExists(OUTPUT_TOPIC, 3, (short) 1);

        StreamsBuilder builder = new StreamsBuilder();

        // Processamento do tópico Routes_topic
        KStream<String, String> routesStream = builder.stream("Routes_topic");

        KTable<String, Integer> routeCapacities = routesStream
            .mapValues(value -> {
                try {
                    JSONObject json = new JSONObject(value);
                    JSONObject payload = json.getJSONObject("payload");
                    String routeId = payload.getString("routeId");
                    int capacity = payload.getInt("capacity");
                    return routeId + ":" + capacity;
                } catch (Exception e) {
                    System.err.println("Erro ao parsear JSON: " + e.getMessage());
                    return null;
                }
            })
            .filter((key, value) -> value != null) // Filtra mensagens inválidas
            .groupBy((key, value) -> value.split(":")[0]) // Agrupa pelo routeId
            .aggregate(
                () -> 0,
                (key, value, aggregate) -> Integer.parseInt(value.split(":")[1]),
                Materialized.with(Serdes.String(), Serdes.Integer())
            );

        // Processamento do tópico Trips_topic
        KStream<String, String> tripsStream = builder.stream("Trips_topic");

        KTable<String, Long> passengersPerRoute = tripsStream
            .mapValues(value -> {
                try {
                    JSONObject json = new JSONObject(value);
                    JSONObject payload = json.getJSONObject("payload");
                    return payload.getString("routeId");
                } catch (Exception e) {
                    System.err.println("Erro ao parsear JSON: " + e.getMessage());
                    return null;
                }
            })
            .filter((key, routeId) -> routeId != null) // Filtra mensagens inválidas
            .groupBy((key, routeId) -> routeId) // Agrupa por routeId
            .count(Materialized.with(Serdes.String(), Serdes.Long()));

        // Cálculo dos assentos disponíveis por rota
        KTable<String, Integer> availableSeatsPerRoute = routeCapacities.leftJoin(
            passengersPerRoute,
            (capacity, passengers) -> capacity - (passengers != null ? passengers.intValue() : 0)
        );

        // Envio dos dados para o tópico projeto3_available_seats_per_route
        availableSeatsPerRoute.toStream()
            .mapValues((routeId, availableSeats) -> {
                String schema = """
                    {
                        "type": "struct",
                        "fields": [
                            {"field": "routeId", "type": "string"},
                            {"field": "availableSeats", "type": "int32"}
                        ]
                    }
                """;

                String payload = String.format(
                    "{\"routeId\": \"%s\", \"availableSeats\": %d}",
                    routeId, availableSeats
                );

                return String.format("{\"schema\": %s, \"payload\": %s}", schema, payload);
            })
            .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static void createTopicIfNotExists(String topicName, int numPartitions, short replicationFactor) {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9093,broker3:9094");

        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            if (!adminClient.listTopics().names().get().contains(topicName)) {
                NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor);
                adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
                System.out.printf("Tópico %s criado com sucesso!%n", topicName);
            } else {
                System.out.printf("Tópico %s já existe.%n", topicName);
            }
        } catch (InterruptedException | ExecutionException e) {
            System.err.printf("Erro ao verificar/criar o tópico %s: %s%n", topicName, e.getMessage());
        }
    }
}