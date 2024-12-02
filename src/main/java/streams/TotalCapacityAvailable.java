package streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.KeyValue;
import org.json.JSONObject;
import utils.KafkaTopicUtils;

import java.util.Properties;

public class TotalCapacityAvailable {

    private static final String OUTPUT_TOPIC = "projeto3_total_capacity_available";
    private static final String INPUT_ROUTES_TOPIC = "Routes_topic";

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "total-capacity-available-app3");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9093,broker3:9094");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        KafkaTopicUtils topicUtils = new KafkaTopicUtils(props);
        topicUtils.createTopicIfNotExists(OUTPUT_TOPIC, 3, (short) 1);

        StreamsBuilder builder = new StreamsBuilder();

        //Capacidades por rota
        KStream<String, String> routesStream = builder.stream(INPUT_ROUTES_TOPIC);

        KTable<String, Integer> routeCapacities = routesStream
                .mapValues(value -> {
                    try {
                        JSONObject json = new JSONObject(value);
                        JSONObject payload = json.getJSONObject("payload");
                        return payload.getInt("capacity");
                    } catch (Exception e) {
                        System.err.println("Erro ao parsear JSON: " + e.getMessage());
                        return null;
                    }
                })
                .filter((routeId, capacity) -> capacity != null)
                .groupByKey()
                .aggregate(
                        () -> 0, 
                        (routeId, newCapacity, totalCapacity) -> newCapacity,
                        Materialized.with(Serdes.String(), Serdes.Integer())
                );

        //Somar todas as capacidades
        KTable<String, Integer> totalCapacity = routeCapacities
            .groupBy(
                (routeId, capacity) -> KeyValue.pair("total", capacity), //Agrupa todas as rotas na chave total
                Grouped.with(Serdes.String(), Serdes.Integer()) 
            )
            .reduce(
                Integer::sum,
                Integer::sum,
                Materialized.with(Serdes.String(), Serdes.Integer())
            );
        

        //Escrever o resultado no topico
        totalCapacity.toStream()
                .mapValues((key, totalCapacityValue) -> {
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