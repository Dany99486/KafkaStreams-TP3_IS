package streamFunctions;

import classes.Trip;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import utils.JsonDeserializer;
import utils.JsonSerializer;
import utils.KafkaTopicUtils;

public class TransportTypeMaxPassengers {

    private static final String INPUT_TRIPS_TOPIC = "Trips_topic";
    private static final String OUTPUT_TOPIC = "projeto3_max_transport_type";

    public static void addTransportTypeMaxPassengersStream(StreamsBuilder builder, KafkaTopicUtils topicUtils) {

        topicUtils.createTopicIfNotExists(OUTPUT_TOPIC, 3, (short) 1);

        KStream<String, Trip> tripsStream = builder.stream(
                INPUT_TRIPS_TOPIC,
                Consumed.with(Serdes.String(), Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(Trip.class)))
        );

        //Conta viagens por tipo de transporte
        KTable<String, Long> passengersByTransportType = tripsStream
                .filter((key, trip) -> trip != null && trip.getTransportType() != null)
                .groupBy((key, trip) -> trip.getTransportType()) //Agrupa por tipo de transporte
                .count(); // Conta o número de viagens

        //Encontra o tipo de transporte com o maior número de passageiros
        KStream<String, String> maxTransportTypeStream = passengersByTransportType
                .toStream()
                .map((transportType, passengerCount) -> KeyValue.pair("maxPassengersTransportType",
                        transportType + ":" + passengerCount))
                .groupByKey() //Agrupa pela chave fixa
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

        maxTransportTypeStream
                .mapValues(value -> {
                    String[] parts = value.split(":");
                    String transportType = parts[0];

                    String schema = """
                        {
                            "type": "struct",
                            "fields": [
                                {"field": "transportType", "type": "string"}
                            ]
                        }
                    """;

                    String payload = String.format(
                            "{\"transportType\": \"%s\"}",
                            transportType
                    );

                    return String.format("{\"schema\": %s, \"payload\": %s}", schema, payload);
                })
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

    }
}