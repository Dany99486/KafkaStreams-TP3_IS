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

public class AveragePassengersPerTransportType {

    private static final String OUTPUT_TOPIC = "projeto3_average_passengers_per_transport_types";
    private static final String INPUT_TRIPS_TOPIC = "Trips_topic";

    public static void main(String[] args) {
        // Configuração para Kafka Streams
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "average-passengers-across-transport-types-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9093,broker3:9094");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        KafkaTopicUtils topicUtils = new KafkaTopicUtils(props);
        topicUtils.createTopicIfNotExists(OUTPUT_TOPIC, 3, (short) 1);

        StreamsBuilder builder = new StreamsBuilder();

        // Read input stream
        KStream<String, String> tripsStream = builder.stream(INPUT_TRIPS_TOPIC);

        // Extract transport type and map it to a passenger count
        KStream<String, String> transportTypeStream = tripsStream
                .mapValues((String value) -> {
                    try {
                        JSONObject json = new JSONObject(value);
                        JSONObject payload = json.getJSONObject("payload");
                        return payload.getString("transportType");
                    } catch (Exception e) {
                        System.err.println("Erro ao parsear JSON: " + e.getMessage());
                        return null;
                    }
                })
                .filter((key, transportType) -> transportType != null);

        // Contar o número total de passageiros por tipo de transporte
        KTable<String, Long> totalPassengersByTransportType = transportTypeStream
                .groupBy((key, transportType) -> "key")  // Agrupa todas as mensagens em uma única chave
                .count();


        KTable<String, Long> distinctTransportTypeCount = transportTypeStream
                .groupBy((key, transportType) -> transportType)  // Agrupa por tipo de transporte
                .aggregate(
                        () -> "",  // Função inicial: inicia com 0 para cada tipo
                        (key, transportType, aggregate) -> transportType  // Função de agregação: marca presença como 1 para cada tipo distinto
                )
                .toStream()  // Converte a KTable em KStream
                .groupBy((key, value) -> "key")  // Agrupa todas as mensagens em uma única chave
                .count();  // Conta o número total de tipos distintos

        KTable<String, Double> averagePassangerPerTransporte = totalPassengersByTransportType.join(
                distinctTransportTypeCount,
                (totalPassengers, distinctTypes) -> (double) totalPassengers / distinctTypes
        );

        // Emit the results to the output topic
        averagePassangerPerTransporte.toStream()
                .mapValues((key, average) -> {
                    String schema = """
                    {
                        "type": "struct",
                        "fields": [
                            {"field": "averagePassengers", "type": "double"}
                        ]
                    }
                """;

                    String payload = String.format(
                            "{\"averagePassengers\": %.2f}",
                            average
                    );

                    return String.format("{\"schema\": %s, \"payload\": %s}", schema, payload);
                })
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        // Start the Kafka Streams application
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            topicUtils.close();
        }));
    }
}
