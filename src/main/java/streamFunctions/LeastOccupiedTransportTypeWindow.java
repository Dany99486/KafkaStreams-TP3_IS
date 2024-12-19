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


        var routeSerde = Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(Route.class));
        var tripSerde = Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(Trip.class));


        KStream<String, Route> routesStream = builder.stream(
                INPUT_ROUTES_TOPIC,
                Consumed.with(Serdes.String(), routeSerde)
        );

        KStream<String, Trip> tripsStream = builder.stream(
                INPUT_TRIPS_TOPIC,
                Consumed.with(Serdes.String(), tripSerde)
        );

        //Agrupa e soma a capacidade total por tipo de transporte (sem janela)
        KTable<String, Integer> totalCapacityByTransportType = routesStream
                .filter((key, route) -> route != null && route.getTransportType() != null)
                .groupBy((key, route) -> route.getTransportType(), Grouped.with(Serdes.String(), routeSerde))
                .aggregate(
                        () -> 0,
                        (transportType, route, totalCapacity) -> totalCapacity + route.getCapacity(),
                        Materialized.with(Serdes.String(), Serdes.Integer())
                );

        //Converte routesStream em KTable (para join com trips)
        KTable<String, Route> routesTable = routesStream
                .filter((key, route) -> route != null && route.getRouteId() != null)
                .groupByKey()
                .reduce((aggValue, newValue) -> newValue);

        //Junta trips com routes para obter o tipo de transporte de cada viagem
        KStream<String, String> tripsWithTransportType = tripsStream
                .filter((key, trip) -> trip != null && trip.getRouteId() != null)
                .selectKey((key, trip) -> trip.getRouteId())
                .join(
                        routesTable,
                        (trip, route) -> route != null ? route.getTransportType() : null,
                        Joined.with(Serdes.String(), tripSerde, routeSerde)
                )
                .filter((key, transportType) -> transportType != null);

        //Contagem de passageiros por tipo de transporte em janelas de 1 hora
        KTable<Windowed<String>, Long> totalPassengersByTransportTypeWindowed = tripsWithTransportType
                .groupBy((routeId, transportType) -> transportType, Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofHours(1), Duration.ZERO))
                .count(Materialized.with(Serdes.String(), Serdes.Long()));

        //Converte em stream para calcular ocupação
        KStream<Windowed<String>, Long> passengersWindowedStream = totalPassengersByTransportTypeWindowed.toStream();

        //Juntar com capacidade
        KStream<String, Double> occupancyByTransportType = passengersWindowedStream
                .selectKey((windowedKey, passengers) -> windowedKey.key()) //A Chave agora é transportType
                .leftJoin(totalCapacityByTransportType,
                        (passengers, capacity) -> {
                            if (capacity == null || capacity == 0) return 0.0;
                            return (passengers.doubleValue() / capacity) * 100.0;
                        }
                );

        //Encontrar o tipo de transporte menos ocupado agregando com leastOccupiedTransportType e selecionando o menor valor
        KTable<String, String> leastOccupiedTransportType = occupancyByTransportType
                .mapValues((transportType, occupancy) -> transportType + ":" + occupancy)
                .groupBy(
                        (transportType, value) -> "leastOccupiedTransportType",
                        Grouped.with(Serdes.String(), Serdes.String()))
                .aggregate(
                        () -> "",
                        (key, newValue, currentValue) -> newValue, //Apresenta sempre o valor mais recente
                        Materialized.with(Serdes.String(), Serdes.String())
                    );                 


        leastOccupiedTransportType.toStream()
                .filter((key, value) -> value != null && !value.isEmpty())
                .mapValues(value -> {
                    String[] parts = value.split(":");
                    String transportType = parts[0];
                    double occupancy = Double.parseDouble(parts[1]);

                    String schema = """
                        {
                            "type": "struct",
                            "fields": [
                                {"field": "transportType", "type": "string", "optional": true},
                                {"field": "occupancy", "type": "double", "optional": true}
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