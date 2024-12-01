package streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.json.JSONObject;
import utils.KafkaTopicUtils;

import java.util.Properties;

public class OccupancyPerRoute {

    private static final String OUTPUT_TOPIC = "projeto3_occupancy_per_route";
    private static final String INPUT_ROUTES_TOPIC = "Routes_topic";
    private static final String INPUT_TRIPS_TOPIC = "Trips_topic";

    public static void main(String[] args) {
        // Configuração para Kafka Streams
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "occupancy-per-route-app15");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9093,broker3:9094");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());


        KafkaTopicUtils topicUtils = new KafkaTopicUtils(props);
        topicUtils.createTopicIfNotExists(OUTPUT_TOPIC, 3, (short) 1);

        StreamsBuilder builder = new StreamsBuilder();

        // Process route capacities
        KStream<String, String> routesStream = builder.stream(INPUT_ROUTES_TOPIC);

        KTable<String, Integer> routeCapacities = routesStream
                .mapValues(value -> {
                    try {
                        // Parse o JSON para extrair a capacidade
                        JSONObject json = new JSONObject(value);
                        JSONObject payload = json.getJSONObject("payload");
                        return payload.getInt("capacity"); // Extrai apenas a capacidade
                    } catch (Exception e) {
                        System.err.println("Erro ao parsear JSON: " + e.getMessage());
                        return null; // Retorna null em caso de erro
                    }
                })
                .filter((routeId, capacity) -> capacity != null) // Filtra mensagens inválidas
                .groupByKey() // Usa a chave já existente no tópico (routeId)
                .aggregate(
                        () -> 0, // Inicializa a capacidade como 0
                        (routeId, newCapacity, currentCapacity) -> newCapacity, // Substitui pelo novo valor
                        Materialized.with(Serdes.String(), Serdes.Integer())
                );// key:route_ID value:capacity

        // Process trips to count passengers
        KStream<String, String> tripsStream = builder.stream(INPUT_TRIPS_TOPIC);

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
                .filter((key, routeId) -> routeId != null) // Filter invalid messages
                .groupBy((key, routeId) -> routeId) // Group by routeId
                .count(Materialized.with(Serdes.String(), Serdes.Long()));

        // Calculate occupancy percentage
        KTable<String, Double> occupancyPercentagePerRoute = routeCapacities.leftJoin(
                passengersPerRoute,
                (capacity, passengers) -> {
                    if (capacity == 0 || passengers == null) return 0.0; // Avoid division by zero
                    return (passengers.doubleValue() / capacity) * 100;
                },
                Materialized.with(Serdes.String(), Serdes.Double())
        );

        // Write results to the output topic
        occupancyPercentagePerRoute.toStream()
                .mapValues((routeId, occupancyPercentage) -> {
                    String schema = """
                    {
                        "type": "struct",
                        "fields": [
                            {"field": "routeId", "type": "string"},
                            {"field": "occupancyPercentage", "type": "double"}
                        ]
                    }
                """;

                    String payload = String.format(
                            "{\"routeId\": \"%s\", \"occupancyPercentage\": %.2f}",
                            routeId, occupancyPercentage
                    );

                    return String.format("{\"schema\": %s, \"payload\": %s}", schema, payload);
                })
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            topicUtils.close();
        }));
    }
}