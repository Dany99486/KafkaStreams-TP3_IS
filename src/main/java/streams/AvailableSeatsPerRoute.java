package streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import utils.JsonDeserializer;
import utils.JsonSerializer;
import utils.KafkaTopicUtils;
import classes.Route;

import java.util.Properties;

public class AvailableSeatsPerRoute {

    private static final String OUTPUT_TOPIC = "projeto3_available_seats_per_route";
    private static final String INPUT_ROUTES_TOPIC = "Routes_topic";

    public static void main(String[] args) {
        // Configuração para Kafka Streams
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "available-seats-per-route-app10");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9093,broker3:9094");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        KafkaTopicUtils topicUtils = new KafkaTopicUtils(props);
        topicUtils.createTopicIfNotExists(OUTPUT_TOPIC, 3, (short) 1);

        StreamsBuilder builder = new StreamsBuilder();

        // Usa serializer e deserializer personalizados para Route
        JsonDeserializer<Route> routeDeserializer = new JsonDeserializer<>(Route.class);
        JsonSerializer<Route> routeSerializer = new JsonSerializer<>();

        // Configura o stream com serializer e deserializer personalizados
        KStream<String, Route> routesStream = builder.stream(
            INPUT_ROUTES_TOPIC,
            Consumed.with(Serdes.String(), Serdes.serdeFrom(routeSerializer, routeDeserializer))
        );

        // Soma das capacidades das rotas agrupadas por routeId
        KTable<String, Integer> routeCapacities = routesStream
            .filter((key, route) -> route != null && route.getRouteId() != null) //Filtra mensagens inválidas
            .groupBy((key, route) -> route.getRouteId()) //Agrupa por routeId
            .aggregate(
                () -> 0, // Capacidade inicial = 0
                (routeId, route, totalCapacity) -> totalCapacity + route.getCapacity(), // Soma as capacidades
                Materialized.with(Serdes.String(), Serdes.Integer())
            );

        // Envia o resultado para o tópico de saída
        routeCapacities.toStream()
            .mapValues(totalCapacity -> {
                String schema = """
                    {
                        "type": "struct",
                        "fields": [
                            {"field": "totalCapacity", "type": "int32"}
                        ]
                    }
                """;

                String payload = String.format(
                    "{\"totalCapacity\": %d}",
                    totalCapacity
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