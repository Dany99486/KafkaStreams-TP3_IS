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

        Serde<Route> routeSerde = Serdes.serdeFrom(routeSerializer, routeDeserializer);
        Serde<Trip> tripSerde = Serdes.serdeFrom(tripSerializer, tripDeserializer);

        Serde<Long> longSerde = Serdes.Long();
        Serde<Integer> integerSerde = Serdes.Integer();
        Serde<Double> doubleSerde = Serdes.Double();
        Serde<String> stringSerde = Serdes.String();


        KStream<String, Route> routesStream = builder.stream(
                        INPUT_ROUTES_TOPIC,
                        Consumed.with(Serdes.String(), routeSerde)
                )
                .filter((key, route) -> route != null && route.getRouteId() != null && route.getOperator() != null)
                .peek((key, route) -> System.out.println("Routes Stream - Key: " + key + ", Route: " + route));


                KStream<String, Trip> tripsStream = builder.stream(
                        INPUT_TRIPS_TOPIC,
                        Consumed.with(Serdes.String(), tripSerde)
                )
                .filter((key, trip) -> trip != null && trip.getRouteId() != null)
                .peek((key, trip) -> System.out.println("Trips Stream - Key: " + key + ", Trip: " + trip));

        //Mapeia Route IDs para operadores
        KTable<String, String> routeToOperator = routesStream
                .groupBy(
                        (key, route) -> route.getRouteId(),
                        Grouped.with(stringSerde, routeSerde)
                )
                .aggregate(
                        () -> null,
                        (routeId, route, currentValue) -> route.getOperator(),
                        Materialized.with(stringSerde, Serdes.String())
                )
                .toStream()
                .peek((routeId, operator) -> System.out.println("Route to Operator - RouteID: " + routeId + ", Operator: " + operator))
                .toTable(Materialized.with(stringSerde, Serdes.String()));

        //Conta passageiros por rota
        KTable<String, Long> passengersPerRoute = tripsStream
                .groupBy(
                        (key, trip) -> trip.getRouteId(),
                        Grouped.with(stringSerde, tripSerde)
                )
                .count(Materialized.with(stringSerde, longSerde))
                .toStream()
                .peek((routeId, passengerCount) -> System.out.println("Passengers per Route - RouteID: " + routeId + ", Count: " + passengerCount))
                .toTable(Materialized.with(stringSerde, longSerde));

        //Junção de passengersPerRoute com routeToOperator para obter operador e contagem de passageiros
        KStream<String, KeyValue<String, Long>> operatorPassengerStream = passengersPerRoute
                .toStream()
                .join(
                        routeToOperator,
                        (passengerCount, operator) -> new KeyValue<>(operator, passengerCount),
                        Joined.with(stringSerde, longSerde, Serdes.String())
                );

        //Mapeia para operador como chave e passengerCount como valor
        KStream<String, Long> operatorPassengerMappedStream = operatorPassengerStream
                .map((routeId, operatorPassenger) -> {
                    String operator = operatorPassenger.key;
                    Long passengerCount = operatorPassenger.value;
                    System.out.println("Joined Table - RouteID: " + routeId + ", Operator: " + operator + ", Passengers: " + passengerCount);
                    return new KeyValue<>(operator, passengerCount);
                });

        //Agrupa por operador e soma os passageiros
        KTable<String, Long> operatorPassengers = operatorPassengerMappedStream
                .groupBy(
                        (operator, passengerCount) -> operator,
                        Grouped.with(stringSerde, longSerde)
                )
                .aggregate(
                        //Valor inicial é zero
                        () -> 0L,
                        //Atualiza o valor atual sempre com o mais recente
                        (operator, newValue, oldValue) -> newValue,
                        Materialized.with(stringSerde, longSerde)
                );

        operatorPassengers
                .toStream()
                .peek((operator, totalPassengers) -> System.out.println("Operator Passengers - Operator: " + operator + ", Total Passengers: " + totalPassengers));

        // Soma capacidades por operador
        KTable<String, Integer> operatorCapacities = routesStream
                .map((key, route) -> new KeyValue<>(route.getOperator(), route.getCapacity()))
                .groupByKey(Grouped.with(stringSerde, integerSerde))
                .reduce(
                        Integer::sum,
                        Materialized.with(stringSerde, integerSerde)
                )
                .toStream()
                .peek((operator, totalCapacity) -> System.out.println("Operator Capacities - Operator: " + operator + ", Total Capacity: " + totalCapacity))
                .toTable(Materialized.with(stringSerde, integerSerde));

        // Calcula ocupação por operador
        KTable<String, Double> operatorOccupancyPercentage = operatorCapacities.join(
                operatorPassengers,
                (totalCapacity, totalPassengers) -> {
                    if (totalCapacity == 0 || totalPassengers == null) return 0.0;
                    return ((double) totalPassengers / totalCapacity) * 100;
                },
                Materialized.with(stringSerde, doubleSerde)
                )
                .toStream()
                .peek((operator, occupancy) -> System.out.println("Operator Occupancy - Operator: " + operator + ", Occupancy Percentage: " + occupancy))
                .toTable(Materialized.with(stringSerde, doubleSerde));

        //Determinar operador com maior ocupação
        operatorOccupancyPercentage.toStream()
                .map((operator, occupancyPercentage) -> KeyValue.pair("most_occupied_operator", operator + ":" + occupancyPercentage))
                .peek((key, value) -> System.out.println("Mapped for Aggregation - Key: " + key + ", Value: " + value))
                .groupByKey(Grouped.with(stringSerde, stringSerde)) //Agrupaçã pela chave fixa
                .aggregate(
                        () -> "",
                        (key, newValue, currentMax) -> {
                            if (currentMax.isEmpty()) {
                                return newValue;
                            }
                            String[] currentParts = currentMax.split(":");
                            String[] newParts = newValue.split(":");

                            double currentOccupancy = currentParts.length > 1 ? Double.parseDouble(currentParts[1]) : 0.0;
                            double newOccupancy = newParts.length > 1 ? Double.parseDouble(newParts[1]) : 0.0;

                            //Retorna o operador com maior ocupação
                            return newOccupancy > currentOccupancy ? newValue : currentMax;
                        },
                        Materialized.with(stringSerde, stringSerde)
                )
                .toStream()
                .filter((key, value) -> !value.isEmpty())
                .map((key, value) -> {
                    System.out.println("Aggregated Max - Key: " + key + ", Value: " + value);
                    return new KeyValue<>(key, value);
                })
                .mapValues(value -> {
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
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
    }
}