package streamFunctions;

import classes.Route;
import classes.Trip;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import utils.JsonDeserializer;
import utils.JsonSerializer;
import utils.KafkaTopicUtils;

import java.time.Duration;

public class LeastOccupiedRouteWindow {

    private static final String INPUT_ROUTES_TOPIC = "Routes_topic";
    private static final String INPUT_TRIPS_TOPIC = "Trips_topic";
    private static final String OUTPUT_TOPIC = "projeto3_least_occupancy_per_route";

    public static void addLeastOccupiedRouteWindowStream(StreamsBuilder builder, KafkaTopicUtils topicUtils) {

        topicUtils.createTopicIfNotExists(OUTPUT_TOPIC, 3, (short) 1);

        // Serdes para Route e Trip
        JsonSerializer<Route> routeSerializer = new JsonSerializer<>();
        JsonDeserializer<Route> routeDeserializer = new JsonDeserializer<>(Route.class);
        JsonSerializer<Trip> tripSerializer = new JsonSerializer<>();
        JsonDeserializer<Trip> tripDeserializer = new JsonDeserializer<>(Trip.class);

        // Stream de rotas
        KStream<String, Route> routesStream = builder.stream(
                INPUT_ROUTES_TOPIC,
                Consumed.with(Serdes.String(), Serdes.serdeFrom(routeSerializer, routeDeserializer))
        );

        // Stream de trips
        KStream<String, Trip> tripsStream = builder.stream(
                INPUT_TRIPS_TOPIC,
                Consumed.with(Serdes.String(), Serdes.serdeFrom(tripSerializer, tripDeserializer))
        );

        // Tabela de rotas (para consulta posterior)
        KTable<String, Route> routesTable = routesStream
                .filter((k, v) -> v != null && v.getRouteId() != null)
                .groupByKey(Grouped.with(Serdes.String(), Serdes.serdeFrom(routeSerializer, routeDeserializer)))
                .reduce((oldVal, newVal) -> newVal, Materialized.as("routes-store"));

        // Capacidades por rota
        KTable<String, Integer> routeCapacities = routesStream
                .filter((key, route) -> route != null && route.getRouteId() != null)
                .groupBy((key, route) -> route.getRouteId(), 
                        Grouped.with(Serdes.String(), Serdes.serdeFrom(routeSerializer, routeDeserializer)))
                .reduce(
                        (oldRoute, newRoute) -> newRoute, // Mantém apenas a última rota recebida
                        Materialized.as("latest-route-store")
                )
                // Agora extrai apenas a capacidade da última rota conhecida
                .mapValues(route -> route.getCapacity());

        // Janela de tempo
        TimeWindows oneHourWindow = TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(3));

        // Contagem de passageiros por rota em janela
        KTable<Windowed<String>, Long> passengersPerRouteWindowed = tripsStream
                .filter((key, trip) -> trip != null && trip.getRouteId() != null)
                .groupBy((key, trip) -> trip.getRouteId(), Grouped.with(Serdes.String(), Serdes.serdeFrom(tripSerializer, tripDeserializer)))
                .windowedBy(oneHourWindow)
                .count(Materialized.with(Serdes.String(), Serdes.Long()));

        // Converter a KTable windowed em stream e juntar com routes para obter transportType
        KStream<String, String> occupancyStream = passengersPerRouteWindowed.toStream()
                // Convert from Windowed<String> to String key, adding window info to the value
                .map((windowedKey, passengers) -> {
                        String routeId = windowedKey.key();
                        long windowStart = windowedKey.window().start();
                        long windowEnd = windowedKey.window().end();
                        String value = "passengers=" + passengers +
                                "&windowStart=" + windowStart +
                                "&windowEnd=" + windowEnd +
                                "&routeId=" + routeId;
                        return KeyValue.pair(routeId, value);
                })

                // Join with routesTable to get transportType
                .join(
                        routesTable,
                        (value, route) -> {
                        String transportType = (route != null && route.getTransportType() != null)
                                ? route.getTransportType() : "unknown";
                        return value + "&transportType=" + transportType;
                        }
                )

                // Join with routeCapacities to calculate occupancy
                .join(
                        routeCapacities,
                        (value, capacity) -> {
                        long passengers = extractLong(value, "passengers");
                        double occupancy = 0.0;
                        if (capacity != null && capacity > 0) {
                                occupancy = (passengers / (double) capacity) * 100.0;
                        }
                        return value + "&capacity=" + capacity + "&occupancy=" + occupancy;
                        }
                );


        // Agora criamos uma chave composta com (transportType, windowStart, windowEnd)
        KStream<String, String> compositeKeyStream = occupancyStream.map((routeId, value) -> {
            String transportType = extractString(value, "transportType");
            long windowStart = extractLong(value, "windowStart");
            long windowEnd = extractLong(value, "windowEnd");
            double occupancy = extractDouble(value, "occupancy");

            String compositeKey = windowStart + ":" + windowEnd;
            // Mantém transportType, windowStart, windowEnd e occupancy no value
            String newValue = "transportType=" + transportType + "&windowStart=" + windowStart +
                    "&windowEnd=" + windowEnd + "&occupancy=" + occupancy;
            return KeyValue.pair(compositeKey, newValue);
        });

        // Agrupamos por essa chave composta (transportType:windowStart:windowEnd)
        KGroupedStream<String, String> groupedByCompositeKey = compositeKeyStream.groupByKey();

        // Encontrar o mínimo occupancy por chave composta (ou seja, por tipo e intervalo)
        KTable<String, String> minOccupancyByTransportAndWindow = groupedByCompositeKey.aggregate(
                () -> "occupancy=99999999",
                (key, newValue, aggValue) -> {
                    double newOcc = extractDouble(newValue, "occupancy");
                    double aggOcc = extractDouble(aggValue, "occupancy");
                    if (newOcc < aggOcc) {
                        return newValue; 
                    } else {
                        return aggValue; 
                    }
                },
                Materialized.with(Serdes.String(), Serdes.String())
        );

        // Agora emitimos o resultado final
        minOccupancyByTransportAndWindow.toStream()
                .mapValues(value -> {
                    String transportType = extractString(value, "transportType");
                    long windowStart = extractLong(value, "windowStart");
                    long windowEnd = extractLong(value, "windowEnd");
                    double leastOccupancy = extractDouble(value, "occupancy");

                    String schema = """
                        {
                            "type": "struct",
                            "fields": [
                                {"field": "transportType", "type": "string"},
                                {"field": "windowStart", "type": "double"},
                                {"field": "windowEnd", "type": "double"},
                                {"field": "leastOccupancy", "type": "double"}
                            ]
                        }
                    """;

                    String payload = String.format(
                            "{\"transportType\": \"%s\", \"windowStart\": %d, \"windowEnd\": %d, \"leastOccupancy\": %.2f}",
                            transportType,
                            windowStart,
                            windowEnd,
                            leastOccupancy
                    );

                    return "{\"schema\": " + schema + ", \"payload\": " + payload + "}";
                })
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
        
    }

    // Funções auxiliares para extrair valores de strings
    private static long extractLong(String value, String key) {
        String prefix = key + "=";
        for (String part : value.split("&")) {
            if (part.startsWith(prefix)) {
                return Long.parseLong(part.substring(prefix.length()));
            }
        }
        return 0;
    }

    private static double extractDouble(String value, String key) {
        String prefix = key + "=";
        for (String part : value.split("&")) {
            if (part.startsWith(prefix)) {
                return Double.parseDouble(part.substring(prefix.length()));
            }
        }
        return 0.0;
    }

    private static String extractString(String value, String key) {
        String prefix = key + "=";
        for (String part : value.split("&")) {
            if (part.startsWith(prefix)) {
                return part.substring(prefix.length());
            }
        }
        return "";
    }
}