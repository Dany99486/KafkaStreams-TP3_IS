package streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import classes.Trip;

import org.apache.kafka.streams.kstream.Consumed;
import utils.JsonDeserializer;
import utils.JsonSerializer;
import utils.KafkaTopicUtils;

import java.util.Properties;

public class PassengersPerRoute {

    private static final String OUTPUT_TOPIC = "projeto3_passengers_per_route";
    private static final String INPUT_TRIPS_TOPIC = "Trips_topic";

    public static void main(String[] args) {
        // Configuração para Kafka Streams
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "passengers-per-route-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9093,broker3:9094");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        KafkaTopicUtils topicUtils = new KafkaTopicUtils(props);
        topicUtils.createTopicIfNotExists(OUTPUT_TOPIC, 3, (short) 1);

        StreamsBuilder builder = new StreamsBuilder();

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
                .filter((key, trip) -> trip != null && trip.getRouteId() != null) // Filtra mensagens inválidas
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

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            topicUtils.close();
        }));
    }
}