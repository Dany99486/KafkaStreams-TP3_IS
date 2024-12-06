package streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import utils.JsonDeserializer;
import utils.JsonSerializer;
import utils.KafkaTopicUtils;
import classes.Route;
import classes.Trip;

import java.util.Properties;

public class OccupancyPerRoute {

    private static final String OUTPUT_TOPIC = "projeto3_occupancy_per_route";
    private static final String INPUT_ROUTES_TOPIC = "Routes_topic";
    private static final String INPUT_TRIPS_TOPIC = "Trips_topic";

    public static void main(String[] args) {
        // Configuração para Kafka Streams
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "occupancy-per-route-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9093,broker3:9094");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        KafkaTopicUtils topicUtils = new KafkaTopicUtils(props);
        topicUtils.createTopicIfNotExists(OUTPUT_TOPIC, 3, (short) 1);

        StreamsBuilder builder = new StreamsBuilder();

        // Usa JsonSerializer e JsonDeserializer diretamente
        JsonSerializer<Route> routeSerializer = new JsonSerializer<>();
        JsonDeserializer<Route> routeDeserializer = new JsonDeserializer<>(Route.class);
        JsonSerializer<Trip> tripSerializer = new JsonSerializer<>();
        JsonDeserializer<Trip> tripDeserializer = new JsonDeserializer<>(Trip.class);

        // Configura streams para Routes e Trips
        KStream<String, Route> routesStream = builder.stream(
                INPUT_ROUTES_TOPIC,
                Consumed.with(Serdes.String(), Serdes.serdeFrom(routeSerializer, routeDeserializer))
        );

        KStream<String, Trip> tripsStream = builder.stream(
                INPUT_TRIPS_TOPIC,
                Consumed.with(Serdes.String(), Serdes.serdeFrom(tripSerializer, tripDeserializer))
        );

        // Processar capacidades de rotas
        KTable<String, Integer> routeCapacities = routesStream
                .filter((key, route) -> route != null && route.getRouteId() != null)
                .groupBy((key, route) -> route.getRouteId())
                .aggregate(
                        () -> 0,
                        (routeId, route, totalCapacity) -> totalCapacity + route.getCapacity(),
                        Materialized.with(Serdes.String(), Serdes.Integer())
                );

        //Processar passageiros por rota
        KTable<String, Long> passengersPerRoute = tripsStream
                .filter((key, trip) -> trip != null && trip.getRouteId() != null)
                .groupBy((key, trip) -> trip.getRouteId())
                .count(Materialized.with(Serdes.String(), Serdes.Long()));

        //Calcular porcentagem de ocupação
        KTable<String, Double> occupancyPercentagePerRoute = routeCapacities.leftJoin(
                passengersPerRoute,
                (capacity, passengers) -> {
                    if (capacity == 0 || passengers == null) return 0.0;
                    return (passengers.doubleValue() / capacity) * 100;
                },
                Materialized.with(Serdes.String(), Serdes.Double())
        );

        // Escrever resultado no tópico de saída
        occupancyPercentagePerRoute.toStream()
                .mapValues(occupancyPercentage -> {
                    String schema = """
                        {
                            "type": "struct",
                            "fields": [
                                {"field": "occupancyPercentage", "type": "double"}
                            ]
                        }
                    """;

                    String payload = String.format(
                            "{\"occupancyPercentage\": %.2f}",
                            occupancyPercentage
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