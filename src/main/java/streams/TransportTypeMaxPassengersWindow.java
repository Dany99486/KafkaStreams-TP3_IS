package streams;

import classes.Trip;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import utils.JsonDeserializer;
import utils.JsonSerializer;
import utils.KafkaTopicUtils;

import java.time.Duration;
import java.util.Properties;

public class TransportTypeMaxPassengersWindow {

    private static final String INPUT_TRIPS_TOPIC = "Trips_topic";
    private static final String OUTPUT_TOPIC = "projeto3_max_transport_type_window";

    public static void main(String[] args) {
        // Configuração do Kafka Streams
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "max-transport-type-windowed-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9093,broker3:9094");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        KafkaTopicUtils topicUtils = new KafkaTopicUtils(props);
        topicUtils.createTopicIfNotExists(OUTPUT_TOPIC, 3, (short) 1);

        StreamsBuilder builder = new StreamsBuilder();

        // Consome o tópico de trips
        KStream<String, Trip> tripsStream = builder.stream(
                INPUT_TRIPS_TOPIC,
                Consumed.with(Serdes.String(), Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(Trip.class)))
        );

        // Contagem de viagens por tipo de transporte em uma janela de 1 hora
        KTable<Windowed<String>, Long> passengersByTransportType = tripsStream
                .filter((key, trip) -> trip != null && trip.getTransportType() != null) // Filtra valores nulos
                .groupBy((key, trip) -> trip.getTransportType(), 
                        Grouped.with(Serdes.String(), Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(Trip.class))))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1))) // Tumbling window de 1 hora
                .count(Materialized.with(Serdes.String(), Serdes.Long())); // Conta o número de viagens (passageiros)

        // Encontrar o tipo de transporte com o maior número de passageiros em uma tumbling window
        KStream<String, String> maxTransportTypeStream = passengersByTransportType
                .toStream()
                .map((windowedKey, passengerCount) -> KeyValue.pair(
                        "maxPassengersTransportTypeWindow_" + windowedKey.window().startTime().toString(), // Define a chave da janela
                        windowedKey.key() + ":" + passengerCount)) // Adiciona tipo e contagem como valor temporário
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String())) // Reagrupa pela chave gerada
                .aggregate(
                        () -> "", // Estado inicial vazio
                        (key, newValue, currentMax) -> {
                            // Divide o estado atual e o novo valor para comparar
                            String[] currentParts = currentMax.split(":");
                            String[] newParts = newValue.split(":");

                            long currentCount = currentParts.length > 1 ? Long.parseLong(currentParts[1]) : 0;
                            long newCount = newParts.length > 1 ? Long.parseLong(newParts[1]) : 0;

                            // Retorna o maior entre o atual e o novo
                            return newCount > currentCount ? newValue : currentMax;
                        },
                        Materialized.with(Serdes.String(), Serdes.String())
                )
                .toStream()
                .filter((key, value) -> !value.isEmpty()) // Filtra valores inválidos
                .map((key, value) -> {
                    String[] parts = value.split(":");
                    String transportType = parts[0];
                    long maxPassengers = Long.parseLong(parts[1]);

                    // Formatar o resultado como JSON
                    String schema = """
                        {
                            "type": "struct",
                            "fields": [
                                {"field": "transportType", "type": "string"},
                                {"field": "maxPassengers", "type": "int32"}
                            ]
                        }
                    """;

                    String payload = String.format(
                            "{\"transportType\": \"%s\", \"maxPassengers\": %d}",
                            transportType, maxPassengers
                    );

                    return KeyValue.pair(key, String.format("{\"schema\": %s, \"payload\": %s}", schema, payload));
                });

        // Publicar o resultado no tópico de saída
        maxTransportTypeStream.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        // Inicia o Kafka Streams
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}