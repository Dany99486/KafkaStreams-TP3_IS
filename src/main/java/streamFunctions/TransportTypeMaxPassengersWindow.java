package streamFunctions;

import classes.Trip;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import utils.JsonDeserializer;
import utils.JsonSerializer;
import utils.KafkaTopicUtils;

import java.time.Duration;

public class TransportTypeMaxPassengersWindow {
    private static final String INPUT_TRIPS_TOPIC = "Trips_topic";
    private static final String OUTPUT_TOPIC = "projeto3_max_transport_type_window";

    public static void addTransportTypeMaxPassengersWindowStream(StreamsBuilder builder, KafkaTopicUtils topicUtils) {

        topicUtils.createTopicIfNotExists(OUTPUT_TOPIC, 3, (short) 1);

        KStream<String, Trip> tripsStream = builder.stream(
                INPUT_TRIPS_TOPIC,
                Consumed.with(Serdes.String(), Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(Trip.class)))
        );

        //Conta viagens por tipo de transporte em uma janela de 1 minuto
        KTable<Windowed<String>, Long> passengersByTransportType = tripsStream
                .filter((key, trip) -> trip != null && trip.getTransportType() != null)
                .groupBy((key, trip) -> trip.getTransportType(),
                        Grouped.with(Serdes.String(), Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(Trip.class))))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1))) //Tumbling window de 1 minuto
                .count(Materialized.with(Serdes.String(), Serdes.Long()));

        //Encontra o tipo de transporte com o maior número de passageiros em uma tumbling window
        KStream<String, String> maxTransportTypeStream = passengersByTransportType
                .toStream()
                .map((windowedKey, passengerCount) -> KeyValue.pair(
                        "maxPassengersTransportTypeWindow_" + windowedKey.window().startTime().toString(), //Define a chave da janela
                        windowedKey.key() + ":" + passengerCount))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
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
                .filter((key, value) -> !value.isEmpty())
                .map((key, value) -> {
                    String[] parts = value.split(":");
                    String transportType = parts[0];
                    long maxPassengers = Long.parseLong(parts[1]);

                    String schema = """
                        {
                            \"type\": \"struct\",
                            \"fields\": [
                                {\"field\": \"transportType\", \"type\": \"string\"},
                                {\"field\": \"maxPassengers\", \"type\": \"int32\"}
                            ]
                        }
                    """;
                    String payload = String.format(
                            "{\"transportType\": \"%s\", \"maxPassengers\": %d}",
                            transportType, maxPassengers
                    );

                    return KeyValue.pair("maxPassengersTransportTypeWindow", 
                            String.format("{\"schema\": %s, \"payload\": %s}", schema, payload));
                });

                
        maxTransportTypeStream.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
    }
}