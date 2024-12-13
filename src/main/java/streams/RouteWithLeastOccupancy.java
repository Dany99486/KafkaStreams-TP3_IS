package streams;

import classes.Route;
import classes.Trip;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import utils.JsonDeserializer;
import utils.JsonSerializer;
import utils.KafkaTopicUtils;

public class RouteWithLeastOccupancy {

    private static final String INPUT_TRIPS_TOPIC = "Trips_topic";
    private static final String INPUT_ROUTES_TOPIC = "Routes_topic";
    private static final String OUTPUT_TOPIC = "projeto3_route_least_occupancy";

    public static void addRouteWithLeastOccupancyStream(StreamsBuilder builder, KafkaTopicUtils topicUtils) {
        // Configuração do Kafka Streams

        topicUtils.createTopicIfNotExists(OUTPUT_TOPIC, 3, (short) 1);

        KStream<String, Route> routesStream = builder.stream(
                INPUT_ROUTES_TOPIC,
                Consumed.with(Serdes.String(), Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(Route.class)))
        );

        KStream<String, Trip> tripsStream = builder.stream(
                INPUT_TRIPS_TOPIC,
                Consumed.with(Serdes.String(), Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(Trip.class)))
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

        // Encontrar o route com a menor Occupancy
        KStream<String, String> routeWithLeastOccupancy = occupancyPercentagePerRoute
                .toStream()
                .map((routeID, occupancy) -> KeyValue.pair("leastOccupancyRoute",
                        routeID + ":" + occupancy)) // Adiciona tipo e contagem como valor temporário
                .groupByKey() // Agrupa pela chave fixa "leastOccupancyRoute"
                .aggregate(
                        () -> "", // Estado inicial
                        (key, newValue, currentMax) -> {
                            // Divide o estado atual e o novo valor para comparar
                            String[] currentParts = currentMax.split(":");
                            String[] newParts = newValue.split(":");

                            double currentCount = currentParts.length > 1 ? Double.parseDouble(currentParts[1]) : 0;
                            double newCount = newParts.length > 1 ? Double.parseDouble(newParts[1]) : 0;

                            // Retorna o maior entre o atual e o novo
                            return newCount > currentCount ? newValue : currentMax;
                        },
                        Materialized.with(Serdes.String(), Serdes.String())
                )
                .toStream()
                .filter((key, value) -> !value.isEmpty()); // Filtra valores inválidos

        // Escrever resultado no tópico de saída
        routeWithLeastOccupancy
                .mapValues(value -> {
                    String[] parts = value.split(":");
                    String routeId = parts[0];

                    // Definir o esquema do JSON
                    String schema = """
                        {
                            "type": "struct",
                            "fields": [
                                {"field": "routeId", "type": "string"}
                            ]
                        }
                    """;

                    // Definir o payload do JSON
                    String payload = String.format(
                            "{\"routeId\": \"%s\"}",
                            routeId
                    );

                    // Retorna o JSON completo com schema e payload
                    return String.format("{\"schema\": %s, \"payload\": %s}", schema, payload);
                })
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String())); // Publica o resultado formatado no tópico de saída

    }
}