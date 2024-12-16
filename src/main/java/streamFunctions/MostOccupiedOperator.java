package streamFunctions;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import utils.JsonDeserializer;
import utils.JsonSerializer;
import utils.KafkaTopicUtils;
import classes.Route;
import classes.Trip;

public class MostOccupiedOperator {

    private static final String OUTPUT_TOPIC = "projeto3_most_occupied_operator";
    private static final String INPUT_ROUTES_TOPIC = "Routes_topic";
    private static final String INPUT_TRIPS_TOPIC = "Trips_topic";

    public static void addMostOccupiedOperatorStream(StreamsBuilder builder, KafkaTopicUtils topicUtils) {

        // Cria o tópico de saída se não existir
        topicUtils.createTopicIfNotExists(OUTPUT_TOPIC, 3, (short) 1);

        // Serializadores e deserializadores para Route e Trip
        JsonSerializer<Route> routeSerializer = new JsonSerializer<>();
        JsonDeserializer<Route> routeDeserializer = new JsonDeserializer<>(Route.class);
        JsonSerializer<Trip> tripSerializer = new JsonSerializer<>();
        JsonDeserializer<Trip> tripDeserializer = new JsonDeserializer<>(Trip.class);

        // Serdes para Route e Trip
        Serde<Route> routeSerde = Serdes.serdeFrom(routeSerializer, routeDeserializer);
        Serde<Trip> tripSerde = Serdes.serdeFrom(tripSerializer, tripDeserializer);
        
        // Fluxo de dados para rotas
        KStream<String, Route> routesStream = builder.stream(
                INPUT_ROUTES_TOPIC,
                Consumed.with(Serdes.String(), routeSerde)
        ).map((key, route) -> {
            System.out.println("Routes Stream - Key: " + key + ", Route: " + route);
            return new KeyValue<>(key, route);
        });

        // Fluxo de dados para viagens
        KStream<String, Trip> tripsStream = builder.stream(
                INPUT_TRIPS_TOPIC,
                Consumed.with(Serdes.String(), tripSerde)
        ).map((key, trip) -> {
            System.out.println("Trips Stream - Key: " + key + ", Trip: " + trip);
            return new KeyValue<>(key, trip);
        });

        // Mapeia Route IDs para operadores
        KTable<String, String> routeToOperator = routesStream
                .filter((key, route) -> route != null && route.getRouteId() != null && route.getOperator() != null)
                .groupBy(
                        (key, route) -> route.getRouteId(),
                        Grouped.with(Serdes.String(), routeSerde)
                )
                .aggregate(
                        () -> null,
                        (routeId, route, currentValue) -> route.getOperator(),
                        Materialized.with(Serdes.String(), Serdes.String())
                )
                .toStream()
                .map((routeId, operator) -> {
                    System.out.println("Route to Operator - RouteID: " + routeId + ", Operator: " + operator);
                    return new KeyValue<>(routeId, operator);
                })
                .toTable(Materialized.with(Serdes.String(), Serdes.String()));

        // Conta passageiros por rota
        KTable<String, Long> passengersPerRoute = tripsStream
                .filter((key, trip) -> trip != null && trip.getRouteId() != null)
                .groupBy(
                        (key, trip) -> trip.getRouteId(),
                        Grouped.with(Serdes.String(), tripSerde)
                )
                .count(Materialized.with(Serdes.String(), Serdes.Long()))
                .toStream()
                .map((routeId, passengerCount) -> {
                    System.out.println("Passengers per Route - RouteID: " + routeId + ", Count: " + passengerCount);
                    return new KeyValue<>(routeId, passengerCount);
                })
                .toTable(Materialized.with(Serdes.String(), Serdes.Long()));

        // Convertendo passengersPerRoute para KStream para realizar a junção
        KStream<String, Long> passengersStream = passengersPerRoute.toStream();

        // Realizando a junção com routeToOperator para obter operador e contagem de passageiros
        KStream<String, KeyValue<String, Long>> operatorPassengerStream = passengersStream.join(
                routeToOperator,
                (passengerCount, operator) -> new KeyValue<>(operator, passengerCount),
                Joined.with(Serdes.String(), Serdes.Long(), Serdes.String())
        );

        // Mapeando para operador como chave e passengerCount como valor
        KStream<String, Long> operatorPassengerMappedStream = operatorPassengerStream.map(
                (routeId, operatorPassenger) -> {
                    String operator = operatorPassenger.key;
                    Long passengerCount = operatorPassenger.value;
                    System.out.println("Joined Table - RouteID: " + routeId + ", Operator: " + operator + ", Passengers: " + passengerCount);
                    return new KeyValue<>(operator, passengerCount);
                }
        );

        // Agrupando por operador e somando os passageiros
        KTable<String, Long> operatorPassengers = operatorPassengerMappedStream
                .groupBy(
                        (operator, passengerCount) -> operator,
                        Grouped.with(Serdes.String(), Serdes.Long())
                )
                .reduce(
                        Long::sum,
                        Materialized.with(Serdes.String(), Serdes.Long())
                );

        // Imprimindo operatorPassengers
        operatorPassengers.toStream()
                .map((operator, totalPassengers) -> {
                    System.out.println("Operator Passengers - Operator: " + operator + ", Total Passengers: " + totalPassengers);
                    return new KeyValue<>(operator, totalPassengers);
                })
                .toTable(Materialized.with(Serdes.String(), Serdes.Long()));

        // Soma capacidades por operador
        KTable<String, Integer> operatorCapacities = routesStream
                .filter((key, route) -> route != null && route.getOperator() != null)
                .map((key, route) -> new KeyValue<>(route.getOperator(), route.getCapacity()))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()))
                .reduce(
                        Integer::sum,
                        Materialized.with(Serdes.String(), Serdes.Integer())
                )
                .toStream()
                .map((operator, totalCapacity) -> {
                    System.out.println("Operator Capacities - Operator: " + operator + ", Total Capacity: " + totalCapacity);
                    return new KeyValue<>(operator, totalCapacity);
                })
                .toTable(Materialized.with(Serdes.String(), Serdes.Integer()));

        // Calcula ocupação por operador
        KTable<String, Double> operatorOccupancyPercentage = operatorCapacities.join(
                operatorPassengers,
                (totalCapacity, totalPassengers) -> {
                    if (totalCapacity == 0 || totalPassengers == null) return 0.0;
                    return ((double) totalPassengers / totalCapacity) * 100;
                },
                Materialized.with(Serdes.String(), Serdes.Double())
        )
        .toStream()
        .map((operator, occupancy) -> {
            System.out.println("Operator Occupancy - Operator: " + operator + ", Occupancy Percentage: " + occupancy);
            return new KeyValue<>(operator, occupancy);
        })
        .toTable(Materialized.with(Serdes.String(), Serdes.Double()));

        // Determina operador com maior ocupação
        operatorOccupancyPercentage.toStream()
                .map((operator, occupancyPercentage) -> KeyValue.pair("most_occupied_operator", operator + ":" + occupancyPercentage)) // Concatena operador e taxa
                .map((key, value) -> {
                    System.out.println("Mapped for Aggregation - Key: " + key + ", Value: " + value);
                    return new KeyValue<>(key, value);
                })
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String())) // Agrupa pela chave fixa
                .aggregate(
                        () -> "", // Estado inicial vazio
                        (key, newValue, currentMax) -> {
                            if (currentMax.isEmpty()) {
                                return newValue;
                            }
                            String[] currentParts = currentMax.split(":");
                            String[] newParts = newValue.split(":");

                            double currentOccupancy = currentParts.length > 1 ? Double.parseDouble(currentParts[1]) : 0.0;
                            double newOccupancy = newParts.length > 1 ? Double.parseDouble(newParts[1]) : 0.0;

                            // Retorna o operador com maior ocupação
                            return newOccupancy > currentOccupancy ? newValue : currentMax;
                        },
                        Materialized.with(Serdes.String(), Serdes.String())
                )
                .toStream()
                .filter((key, value) -> !value.isEmpty()) // Filtra valores não vazios
                .map((key, value) -> {
                    System.out.println("Aggregated Max - Key: " + key + ", Value: " + value);
                    return new KeyValue<>(key, value);
                })
                .mapValues(value -> {
                    // Separa o operador e a taxa de ocupação
                    String[] parts = value.split(":");
                    String operator = parts[0];
                    double occupancyPercentage = parts.length > 1 ? Double.parseDouble(parts[1]) : 0.0;

                    // Esquema JSON
                    String schema = """
                            {
                                "type": "struct",
                                "fields": [
                                    {"field": "operator", "type": "string"},
                                    {"field": "occupancyPercentage", "type": "double"}
                                ]
                            }
                    """;

                    // Payload JSON
                    String payload = String.format(
                            "{\"operator\": \"%s\", \"occupancyPercentage\": %.2f}",
                            operator, occupancyPercentage
                    );

                    // Combina schema e payload
                    String jsonResult = "{\"schema\": " + schema + ", \"payload\": " + payload + "}";
                    System.out.println("Final JSON - " + jsonResult);
                    return jsonResult;
                })
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String())); // Produz no tópico de saída
    }
}