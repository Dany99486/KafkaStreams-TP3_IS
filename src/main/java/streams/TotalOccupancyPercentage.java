package streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.KeyValue;
import utils.JsonDeserializer;
import utils.JsonSerializer;
import classes.Route;
import classes.Trip;
import utils.KafkaTopicUtils;

public class TotalOccupancyPercentage {

    private static final String OUTPUT_TOPIC = "projeto3_total_occupancy_percentage";
    private static final String INPUT_ROUTES_TOPIC = "Routes_topic";
    private static final String INPUT_TRIPS_TOPIC = "Trips_topic";

    public static void addTotalOccupancyPercentageStream(StreamsBuilder builder, KafkaTopicUtils topicUtils) {

        topicUtils.createTopicIfNotExists(OUTPUT_TOPIC, 3, (short) 1);

        // Serializadores e desserializadores personalizados
        JsonSerializer<Route> routeSerializer = new JsonSerializer<>();
        JsonDeserializer<Route> routeDeserializer = new JsonDeserializer<>(Route.class);
        JsonSerializer<Trip> tripSerializer = new JsonSerializer<>();
        JsonDeserializer<Trip> tripDeserializer = new JsonDeserializer<>(Trip.class);

        // Stream de rotas com serializadores/desserializadores personalizados
        KStream<String, Route> routesStream = builder.stream(
                INPUT_ROUTES_TOPIC,
                Consumed.with(Serdes.String(), Serdes.serdeFrom(routeSerializer, routeDeserializer))
        );

        KTable<String, Integer> routeCapacities = routesStream
                .groupBy(
                        (key, route) -> route.getRouteId(),
                        Grouped.with(Serdes.String(), Serdes.serdeFrom(routeSerializer, routeDeserializer))
                )
                .aggregate(
                        () -> 0,
                        (routeId, route, currentCapacity) -> currentCapacity + route.getCapacity(),
                        Materialized.with(Serdes.String(), Serdes.Integer())
                );

        // Stream de viagens com serializadores/desserializadores personalizados
        KStream<String, Trip> tripsStream = builder.stream(
                INPUT_TRIPS_TOPIC,
                Consumed.with(Serdes.String(), Serdes.serdeFrom(tripSerializer, tripDeserializer))
        );

        KTable<String, Long> passengersPerRoute = tripsStream
                .groupBy(
                        (key, trip) -> trip.getRouteId(),
                        Grouped.with(Serdes.String(), Serdes.serdeFrom(tripSerializer, tripDeserializer))
                )
                .count(Materialized.with(Serdes.String(), Serdes.Long()));

        // Calcular capacidade total
        KTable<String, Integer> totalCapacity = routeCapacities
        .groupBy(
                (routeId, capacity) -> KeyValue.pair("total", capacity),
                Grouped.with(Serdes.String(), Serdes.Integer())
        )
        .aggregate(
                () -> 0, // Inicializa com 0
                (key, newValue, aggregate) -> aggregate + newValue, // Soma os valores
                (key, oldValue, aggregate) -> aggregate - oldValue, // Rebalanceamento (opcional)
                Materialized.with(Serdes.String(), Serdes.Integer())
        );

        // Calcular total de passageiros
        KTable<String, Long> totalPassengers = passengersPerRoute
        .groupBy(
                (routeId, passengers) -> KeyValue.pair("total", passengers),
                Grouped.with(Serdes.String(), Serdes.Long())
        )
        .aggregate(
                () -> 0L, // Inicializa com 0L
                (key, newValue, aggregate) -> aggregate + newValue, // Soma os valores
                (key, oldValue, aggregate) -> aggregate - oldValue, // Rebalanceamento (opcional)
                Materialized.with(Serdes.String(), Serdes.Long())
        );

        // Calcular a porcentagem total de ocupação
        KTable<String, Double> totalOccupancyPercentage = totalCapacity
                .join(
                        totalPassengers,
                        (capacity, passengers) -> {
                            if (capacity == 0) return 0.0;
                            return (passengers.doubleValue() / capacity) * 100;
                        },
                        Materialized.with(Serdes.String(), Serdes.Double())
                );

        // Escrever o resultado no tópico de saída
        totalOccupancyPercentage.toStream()
                .filter((key, value) -> key.equals("total")) // Apenas a chave "total"
                .mapValues(percentage -> {
                    String schema = """
                    {
                        "type": "struct",
                        "fields": [
                            {"field": "totalOccupancyPercentage", "type": "double"}
                        ]
                    }
                    """;

                    String payload = String.format("{\"totalOccupancyPercentage\": %.2f}", percentage);

                    return String.format("{\"schema\": %s, \"payload\": %s}", schema, payload);
                })
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

    }
}