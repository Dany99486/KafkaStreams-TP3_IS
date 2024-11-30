package streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.json.JSONObject;

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

        KStream<String, String> tripsStream = builder.stream(INPUT_TRIPS_TOPIC);

        KTable<String, Long> passengersPerRoute = tripsStream
            .mapValues((String value) -> {
                try {
                    JSONObject json = new JSONObject(value);
                    JSONObject payload = json.getJSONObject("payload");
                    return payload.getString("routeId");
                } catch (Exception e) {
                    System.err.println("Erro ao parsear JSON: " + e.getMessage());
                    return null; // Retorna null se houver erro
                }
            })
            .filter((key, routeId) -> routeId != null)
            .groupBy((key, routeId) -> routeId)
            .count();

        passengersPerRoute.toStream()
            .mapValues((routeId, count) -> {
                String schema = """
                    {
                        "type": "struct",
                        "fields": [
                            {"field": "routeId", "type": "string"},
                            {"field": "passengerCount", "type": "int64"}
                        ]
                    }
                """;

                String payload = String.format(
                    "{\"routeId\": \"%s\", \"passengerCount\": %d}",
                    routeId, count
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