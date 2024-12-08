package streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.KeyValue;
import utils.JsonDeserializer;
import utils.JsonSerializer;
import utils.KafkaTopicUtils;
import classes.Route;

import java.util.Properties;

public class TotalCapacityAvailable {

    private static final String OUTPUT_TOPIC = "projeto3_total_capacity_available";
    private static final String INPUT_ROUTES_TOPIC = "Routes_topic";

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "total-capacity-available-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9093,broker3:9094");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        KafkaTopicUtils topicUtils = new KafkaTopicUtils(props);
        topicUtils.createTopicIfNotExists(OUTPUT_TOPIC, 3, (short) 1);

        StreamsBuilder builder = new StreamsBuilder();

        // Usa JsonSerializer e JsonDeserializer para Route
        JsonDeserializer<Route> routeDeserializer = new JsonDeserializer<>(Route.class);
        JsonSerializer<Route> routeSerializer = new JsonSerializer<>();

        // Configura o stream com JsonSerializer e JsonDeserializer
        KStream<String, Route> routesStream = builder.stream(
                INPUT_ROUTES_TOPIC,
                Consumed.with(Serdes.String(), Serdes.serdeFrom(routeSerializer, routeDeserializer))
        );

        // Capacidades por rota
        KTable<String, Integer> routeCapacities = routesStream
        .filter((key, route) -> route != null) // Apenas verifica se o objeto não é nulo
        .groupBy(
                (key, route) -> route.getRouteId(), // Usa o routeId como chave
                Grouped.with(Serdes.String(), Serdes.serdeFrom(routeSerializer, routeDeserializer))
        )
        .aggregate(
                () -> 0,
                (routeId, route, totalCapacity) -> totalCapacity + route.getCapacity(), // Assume que getCapacity() sempre retorna um valor válido
                Materialized.with(Serdes.String(), Serdes.Integer())
        );

        // Soma total das capacidades
        KTable<String, Integer> totalCapacity = routeCapacities
                .groupBy(
                        (routeId, capacity) -> KeyValue.pair("total", capacity), // Agrupa todas as rotas na chave "total"
                        Grouped.with(Serdes.String(), Serdes.Integer())
                )
                .reduce(
                        Integer::sum, // Soma as capacidades
                        (oldValue, newValue) -> oldValue - newValue, // Remove capacidades antigas
                        Materialized.with(Serdes.String(), Serdes.Integer())
                );

        // Escrever o resultado no tópico
        totalCapacity.toStream()
                .mapValues(totalCapacityValue -> {
                    String schema = """
                    {
                        "type": "struct",
                        "fields": [
                            {"field": "totalCapacity", "type": "int32"}
                        ]
                    }
                    """;

                    String payload = String.format("{\"totalCapacity\": %d}", totalCapacityValue);

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