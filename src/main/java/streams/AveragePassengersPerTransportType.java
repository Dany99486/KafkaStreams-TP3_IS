package streams;

import classes.Trip;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.json.JSONObject;

import utils.JsonDeserializer;
import utils.JsonSerializer;
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

        // Usa JsonSerializer e JsonDeserializer para Trip
        JsonDeserializer<Trip> tripDeserializer = new JsonDeserializer<>(Trip.class);
        JsonSerializer<Trip> tripSerializer = new JsonSerializer<>();


        // Read input stream
        KStream<String, Trip> tripsStream = builder.stream(
                INPUT_TRIPS_TOPIC,
                Consumed.with(Serdes.String(), Serdes.serdeFrom(tripSerializer, tripDeserializer))
        );

        // Contar o número total de passageiros por tipo de transporte
        KTable<String, Long> totalPassengersByTransportType = tripsStream
                .groupBy((key, transportType) -> "key")  // Agrupa todas as mensagens em uma única chave
                .count();


        KTable<String, Long> distinctTransportTypeCount = tripsStream
                .groupBy((key, transportType) -> transportType.getTransportType())  // Agrupa por tipo de transporte
                .aggregate(
                        () -> "",  // Função inicial: inicia com 0 para cada tipo
                        (key, transportType, aggregate) -> transportType.getTransportType()  // Função de agregação: marca presença como 1 para cada tipo distinto
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
