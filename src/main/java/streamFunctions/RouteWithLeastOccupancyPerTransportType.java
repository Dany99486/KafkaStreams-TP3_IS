package streamFunctions;

import classes.Route;
import classes.Trip;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import utils.JsonDeserializer;
import utils.JsonSerializer;
import utils.KafkaTopicUtils;

public class RouteWithLeastOccupancyPerTransportType {

    private static final String INPUT_TRIPS_TOPIC = "Trips_topic";
    private static final String INPUT_ROUTES_TOPIC = "Routes_topic";
    private static final String OUTPUT_TOPIC = "projeto3_route_least_occupancy_per_transport_type";

    public static void addRouteWithLeastOccupancyPerTransportTypeStream(StreamsBuilder builder, KafkaTopicUtils topicUtils) {

        topicUtils.createTopicIfNotExists(OUTPUT_TOPIC, 3, (short) 1);

        KStream<String, Route> routesStream = builder.stream(
                INPUT_ROUTES_TOPIC,
                Consumed.with(Serdes.String(), Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(Route.class)))
        );

        KStream<String, Trip> tripsStream = builder.stream(
                INPUT_TRIPS_TOPIC,
                Consumed.with(Serdes.String(), Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(Trip.class)))
        );

        //Capacidades das rotas
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

        //Calcular percentagem de ocupação
        KTable<String, Double> occupancyPercentagePerRoute = routeCapacities.leftJoin(
                passengersPerRoute,
                (capacity, passengers) -> {
                    if (capacity == 0 || passengers == null) return 0.0;
                    return (passengers.doubleValue() / capacity) * 100;
                },
                Materialized.with(Serdes.String(), Serdes.Double())
                );

        //Soma passageiros por operador
        KStream<String, String> occupancyWithTransportType = occupancyPercentagePerRoute
                .join(
                        routesStream.toTable(),
                        (occupancyPercentage, route) -> route.getTransportType()+":"+occupancyPercentage
                )
                .toStream();


        //Encontrar a route com a menor ocupação
        KStream<String, String> routeWithLeastOccupancy = occupancyWithTransportType
                .map((routeID, kv) -> KeyValue.pair(kv.split(":")[0],
                        routeID + ":" + kv.split(":")[1]))
                .groupByKey()
                .aggregate(
                        () -> "",
                        (key, newValue, currentMin) -> {
                            String[] currentParts = currentMin.split(":");
                            String[] newParts = newValue.split(":");

                            double currentCount = currentParts.length > 1 ? Double.parseDouble(currentParts[1]) : Double.MAX_VALUE;
                            double newCount = newParts.length > 1 ? Double.parseDouble(newParts[1]) : Double.MAX_VALUE;

                            return newCount < currentCount ? newValue : currentMin;
                        },
                        Materialized.with(Serdes.String(), Serdes.String())
                )
                .toStream()
                .filter((key, value) -> !value.isEmpty());


        routeWithLeastOccupancy
                .mapValues(value -> {
                    String[] parts = value.split(":");
                    String routeId = parts[0];
                    double occupancy = Double.parseDouble(parts[1]);

                    String schema = """
                        {
                            "type": "struct",
                            "fields": [
                                {"field": "routeId", "type": "string"},
                                {"field": "occupancy", "type": "double"}
                            ]
                        }
                    """;

                    String payload = String.format(
                            "{\"routeId\": \"%s\", \"occupancy\": %.2f}",
                            routeId,occupancy
                    );

                    return String.format("{\"schema\": %s, \"payload\": %s}", schema, payload);
                })
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

    }
}