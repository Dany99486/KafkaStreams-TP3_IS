package streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
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

public class TotalCapacityAvailable {

    private static final String OUTPUT_TOPIC = "projeto3_total_capacity_available";
    private static final String INPUT_ROUTES_TOPIC = "Routes_topic";

    public static void addTotalCapacityAvailableStream(StreamsBuilder builder, KafkaTopicUtils topicUtils) {

        topicUtils.createTopicIfNotExists(OUTPUT_TOPIC, 3, (short) 1);

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

    }
}