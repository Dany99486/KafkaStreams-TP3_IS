package streamFunctions;

import classes.Trip;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import utils.JsonDeserializer;
import utils.JsonSerializer;
import utils.KafkaTopicUtils;

public class AveragePassengersPerTransportType {

    private static final String OUTPUT_TOPIC = "projeto3_average_passengers_per_transport_types";
    private static final String INPUT_TRIPS_TOPIC = "Trips_topic";

    public static void addAveragePassengersPerTransportTypeStream(StreamsBuilder builder, KafkaTopicUtils topicUtils) {


        topicUtils.createTopicIfNotExists(OUTPUT_TOPIC, 3, (short) 1);


        JsonDeserializer<Trip> tripDeserializer = new JsonDeserializer<>(Trip.class);
        JsonSerializer<Trip> tripSerializer = new JsonSerializer<>();


        KStream<String, Trip> tripsStream = builder.stream(
                INPUT_TRIPS_TOPIC,
                Consumed.with(Serdes.String(), Serdes.serdeFrom(tripSerializer, tripDeserializer))
        );

        //Conta o total de passageiros (número de trips) por tipo de transporte
        KTable<String, Long> totalPassengersByTransportType = tripsStream
                .groupBy((key, trip) -> trip.getTransportType()) //Agrupa pelo tipo de transporte
                .count(Materialized.with(Serdes.String(), Serdes.Long())); //Conta o número de trips (passageiros)

        //Conta o número de rotas únicas por tipo de transporte
        KTable<String, Long> totalRoutesByTransportType = tripsStream
                .groupBy((key, trip) -> trip.getTransportType() + ":" + trip.getRouteId()) //Agrupa por transporte e id da rota
                .aggregate(
                        () -> 0L,
                        (key, trip, aggregate) -> 1L,
                        Materialized.with(Serdes.String(), Serdes.Long())
                )
                .groupBy((key, value) -> new KeyValue<>(key.split(":")[0], value),
                        org.apache.kafka.streams.kstream.Grouped.with(Serdes.String(), Serdes.Long()))
                .count(Materialized.with(Serdes.String(), Serdes.Long()));


        //Calcula a média de passageiros por rota para cada tipo de transporte
        KTable<String, Double> averagePassengersPerTransportType = totalPassengersByTransportType
                .join(totalRoutesByTransportType,
                        (passengerCount, routeCount) -> (double) passengerCount / routeCount
                );


        averagePassengersPerTransportType
                .toStream()
                .mapValues((average) -> {
                    String schema = """
                    {
                        "type": "struct",
                        "fields": [
                            {"field": "averagePassengers", "type": "double"}
                        ]
                    }
                    """;

                    String payload = String.format(
                            "{\"averagePassengers\": %.2f}",
                            average
                    );

                    return String.format("{\"schema\": %s, \"payload\": %s}", schema, payload);
                })
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
    }
}