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

public class LeastOccupiedTransportTypeWindow {

    private static final String INPUT_ROUTES_TOPIC = "Routes_topic";
    private static final String INPUT_TRIPS_TOPIC = "Trips_topic";
    private static final String OUTPUT_TOPIC = "projeto3_least_occupied_transport_type_window";

    public static void addLeastOccupiedTransportTypeWindowStream(StreamsBuilder builder, KafkaTopicUtils topicUtils) {
        topicUtils.createTopicIfNotExists(OUTPUT_TOPIC, 3, (short) 1);

        // Serdes para Route e Trip
        var routeSerde = Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(Route.class));
        var tripSerde = Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(Trip.class));

        // Consome streams de rotas e viagens
        KStream<String, Route> routesStream = builder.stream(
                INPUT_ROUTES_TOPIC,
                Consumed.with(Serdes.String(), routeSerde)
        );

        KStream<String, Trip> tripsStream = builder.stream(
                INPUT_TRIPS_TOPIC,
                Consumed.with(Serdes.String(), tripSerde)
        );

        // Agrupa e soma a capacidade total por tipo de transporte
        KTable<String, Integer> totalCapacityByTransportType = routesStream
                .filter((key, route) -> route != null && route.getTransportType() != null)
                .groupBy((key, route) -> route.getTransportType(), Grouped.with(Serdes.String(), routeSerde))
                .aggregate(
                        () -> 0,
                        (transportType, route, totalCapacity) -> totalCapacity + route.getCapacity(),
                        Materialized.with(Serdes.String(), Serdes.Integer())
                );

        // Agrupa e conta o número de passageiros por tipo de transporte
        // Junta tripsStream com routesStream para determinar o tipo de transporte de cada viagem
        KStream<String, String> tripsWithTransportType = tripsStream
                .selectKey((key, trip) -> trip.getRouteId())
                .join(
                        routesStream, // Alinha a chave com routeId
                        (trip, route) -> {
                        if (route == null || route.getTransportType() == null) return null;
                        return route.getTransportType(); // Retorna o tipo de transporte
                        },
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)), // Define uma janela de 5 minutos
                        StreamJoined.with(Serdes.String(), tripSerde, routeSerde) // Configura os Serdes
                );

        // Agrupa as viagens por tipo de transporte para contar passageiros
        KTable<String, Long> totalPassengersByTransportType = tripsWithTransportType
                .groupBy((routeId, transportType) -> transportType, Grouped.with(Serdes.String(), Serdes.String()))
                .count(Materialized.with(Serdes.String(), Serdes.Long())); //passageiros por tipo de transporte


        // Calcula a ocupação por tipo de transporte
        KTable<String, Double> occupancyByTransportType = totalPassengersByTransportType
                .join(totalCapacityByTransportType,
                        (passengers, capacity) -> {
                            if (capacity == null || capacity == 0 || passengers == null) return 0.0;
                            return (passengers.doubleValue() / capacity) * 100.0;
                        },
                        Materialized.with(Serdes.String(), Serdes.Double())
                );

        // Identifica o tipo de transporte com a menor ocupação
        KTable<String, String> leastOccupiedTransportType = occupancyByTransportType
                .toStream()
                .map((transportType, occupancy) -> KeyValue.pair("leastOccupiedTransportType",
                        transportType + ":" + occupancy)) // Adiciona tipo e contagem como valor temporário
                .groupByKey() // Agrupa pela chave fixa "maxTransportType"
                .aggregate(
                        () -> "", // Estado inicial vazio
                        (key, newValue, currentMin) -> {
                            String[] currentParts = currentMin.split(":");
                            String[] newParts = newValue.split(":");

                            double currentCount = currentParts.length > 1 ? Double.parseDouble(currentParts[1]) : Double.MAX_VALUE;
                            double newCount = newParts.length > 1 ? Double.parseDouble(newParts[1]) : Double.MAX_VALUE;

                            // Retorna o maior entre o atual e o novo
                            return newCount < currentCount ? newValue : currentMin;
                        },
                        Materialized.with(Serdes.String(), Serdes.String())
                );

        // Publica o resultado no tópico de saída
        leastOccupiedTransportType.toStream()
                .filter((key, value) -> value != null)
                .mapValues(value -> {
                    String[] parts = value.split(":");
                    String transportType = parts[0];
                    double occupancy = Double.parseDouble(parts[1]);

                    // Formata o resultado como JSON
                    String schema = """
                        {
                            "type": "struct",
                            "fields": [
                                {"field": "transportType", "type": "string"},
                                {"field": "occupancy", "type": "double"}
                            ]
                        }
                    """;

                    String payload = String.format(
                            "{\"transportType\": \"%s\", \"occupancy\": %.2f}",
                            transportType, occupancy
                    );

                    return String.format("{\"schema\": %s, \"payload\": %s}", schema, payload);
                })
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
    }
}