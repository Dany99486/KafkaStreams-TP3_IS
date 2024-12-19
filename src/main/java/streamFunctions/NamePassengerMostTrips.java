package streamFunctions;

import classes.Trip;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import utils.JsonDeserializer;
import utils.JsonSerializer;
import utils.KafkaTopicUtils;

public class NamePassengerMostTrips {

    private static final String INPUT_TRIPS_TOPIC = "Trips_topic";
    private static final String OUTPUT_TOPIC = "projeto3_most_trips_passenger";

    public static void addNamePassengerMostTripsStreams(StreamsBuilder builder, KafkaTopicUtils topicUtils) {

        topicUtils.createTopicIfNotExists(OUTPUT_TOPIC, 3, (short) 1);

        //Consome o tópico de trips
        KStream<String, Trip> tripsStream = builder.stream(
                INPUT_TRIPS_TOPIC,
                Consumed.with(Serdes.String(), Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(Trip.class)))
        );

        //Conta o número de trips por passageiro
        KTable<String, Long> passengerTripCounts = tripsStream
                .filter((key, trip) -> trip != null && trip.getPassengerName() != null)
                .groupBy((key, trip) -> trip.getPassengerName()) //Agrupa por nome do passageiro
                .count();

        //Encontra o passageiro com o maior numero de trips
        KStream<String, String> maxPassenger = passengerTripCounts
                .toStream()
                .map((passengerName, tripCount) -> KeyValue.pair("maxTripsPassenger", passengerName + ":" + tripCount))
                .groupByKey()
                .aggregate(
                        () -> "",
                        (key, newValue, currentMax) -> {
                            String[] currentParts = currentMax.split(":");
                            String[] newParts = newValue.split(":");

                            long currentCount = currentParts.length > 1 ? Long.parseLong(currentParts[1]) : 0;
                            long newCount = newParts.length > 1 ? Long.parseLong(newParts[1]) : 0;

                            
                            return newCount > currentCount ? newValue : currentMax;
                        },
                        Materialized.with(Serdes.String(), Serdes.String())
                )
                .toStream()
                .filter((key, value) -> !value.isEmpty());

        maxPassenger
                .mapValues(value -> {
                    String[] parts = value.split(":");
                    String passengerName = parts[0];

                    String schema = """
                        {
                            "type": "struct",
                            "fields": [
                                {"field": "passengerName", "type": "string"}
                            ]
                        }
                    """;

                    String payload = String.format(
                            "{\"passengerName\": \"%s\"}",
                            passengerName
                    );

                    return String.format("{\"schema\": %s, \"payload\": %s}", schema, payload);
                })
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

    }
}