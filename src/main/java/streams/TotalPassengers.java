package streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.json.JSONObject;
import utils.KafkaTopicUtils;

import java.util.Properties;

public class TotalPassengers {

    private static final String OUTPUT_TOPIC = "projeto3_total_passengers";
    private static final String INPUT_TRIPS_TOPIC = "Trips_topic";
    public static void main(String[] args) {
        // Configuração para Kafka Streams
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "total-passengers-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9093,broker3:9094");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        KafkaTopicUtils topicUtils = new KafkaTopicUtils(props);
        topicUtils.createTopicIfNotExists(OUTPUT_TOPIC, 3, (short) 1);

        StreamsBuilder builder = new StreamsBuilder();

        // Processar trips para calcular o total de passageiros
        KStream<String, String> tripsStream = builder.stream(INPUT_TRIPS_TOPIC);

        KTable<String, Long> totalPassengers = tripsStream
                .groupBy((key, value) -> "totalPassangers") // Agrupa todas as mensagens em uma única chave
                .count(); // Conta o total de mensagens

        // Envia o total para o tópico de saída
        totalPassengers.toStream()
                .mapValues(total -> {
                    // Gera o JSON com o resultado
                    String schema = """
                    {
                        "type": "struct",
                        "fields": [
                            {"field": "totalPassengers", "type": "int64"}
                        ]
                    }
                    """;

                    String payload = String.format(
                            "{\"totalPassengers\": %d}",
                            total
                    );

                    return String.format("{\"schema\": %s, \"payload\": %s}", schema, payload);
                })
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        // Inicia o Kafka Streams
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            topicUtils.close();
        }));
    }
}
