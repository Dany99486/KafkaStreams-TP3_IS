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

public class AvailableSeatsPerRoute {

    private static final String OUTPUT_TOPIC = "projeto3_available_seats_per_route";
    private static final String INPUT_ROUTES_TOPIC = "Routes_topic";
    private static final String INPUT_TRIPS_TOPIC = "Trips_topic";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "available-seats-per-route-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9093,broker3:9094");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        KafkaTopicUtils topicUtils = new KafkaTopicUtils(props);
        topicUtils.createTopicIfNotExists(OUTPUT_TOPIC, 3, (short) 1);

        StreamsBuilder builder = new StreamsBuilder();

        
        KStream<String, String> routesStream = builder.stream(INPUT_ROUTES_TOPIC);

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
            .filter((key, routeId) -> routeId != null) // Filtra mensagens inválidas
            .groupBy((key, routeId) -> routeId) // Agrupa por routeId
            .count(Materialized.with(Serdes.String(), Serdes.Long()));

        KTable<String, Integer> availableSeatsPerRoute = routeCapacities.leftJoin(
            passengersPerRoute,
            (capacity, passengers) -> capacity - (passengers != null ? passengers.intValue() : 0)
        );

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

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            topicUtils.close();
        }));
    }
}