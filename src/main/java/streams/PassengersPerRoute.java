package streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import classes.Trip;

import org.apache.kafka.streams.kstream.Consumed;
import utils.JsonDeserializer;
import utils.JsonSerializer;
import utils.KafkaTopicUtils;

public class PassengersPerRoute {

    private static final String OUTPUT_TOPIC = "projeto3_passengers_per_route";
    private static final String INPUT_TRIPS_TOPIC = "Trips_topic";

    public static void addPassengersPerRouteStream(StreamsBuilder builder, KafkaTopicUtils topicUtils) {

        topicUtils.createTopicIfNotExists(OUTPUT_TOPIC, 3, (short) 1);

        // Usa JsonSerializer e JsonDeserializer para criar o Serde
        JsonSerializer<Trip> tripSerializer = new JsonSerializer<>();
        JsonDeserializer<Trip> tripDeserializer = new JsonDeserializer<>(Trip.class);

        // Cria o Serde personalizado para Trip
        var tripSerde = Serdes.serdeFrom(tripSerializer, tripDeserializer);

        // Configura o stream com o Serde personalizado
        KStream<String, Trip> tripsStream = builder.stream(
            INPUT_TRIPS_TOPIC,
            Consumed.with(Serdes.String(), tripSerde)
        );

        // Lógica de processamento
        KTable<String, Long> passengersPerRoute = tripsStream
                .filter((key, trip) -> trip != null && trip.getRouteId() != null)
                .groupBy((key, trip) -> trip.getRouteId())
                .count();

        passengersPerRoute.toStream()
                .mapValues((routeId, count) -> {
                    String schema = """
                        {
                            "type": "struct",
                            "fields": [
                                {"field": "passengerCount", "type": "int64"}
                            ]
                        }
                    """;

                    String payload = String.format(
                            "{\"passengerCount\": %d}",
                            count
                    );

                    return String.format("{\"schema\": %s, \"payload\": %s}", schema, payload);
                })
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

    }
}