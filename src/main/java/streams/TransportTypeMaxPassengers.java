package streams;

import classes.Trip;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import utils.JsonDeserializer;
import utils.JsonSerializer;
import utils.KafkaTopicUtils;

public class TransportTypeMaxPassengers {

    private static final String INPUT_TRIPS_TOPIC = "Trips_topic";
    private static final String OUTPUT_TOPIC = "projeto3_max_transport_type";

    public static void addTransportTypeMaxPassengersStream(StreamsBuilder builder, KafkaTopicUtils topicUtils) {

        topicUtils.createTopicIfNotExists(OUTPUT_TOPIC, 3, (short) 1);

        // Consome o tópico de trips
        KStream<String, Trip> tripsStream = builder.stream(
                INPUT_TRIPS_TOPIC,
                Consumed.with(Serdes.String(), Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(Trip.class)))
        );

        // Contagem de viagens por tipo de transporte (número de passageiros é igual ao número de viagens)
        KTable<String, Long> passengersByTransportType = tripsStream
                .filter((key, trip) -> trip != null && trip.getTransportType() != null) // Filtra valores nulos
                .groupBy((key, trip) -> trip.getTransportType()) // Agrupa por tipo de transporte
                .count(); // Conta o número de viagens (passageiros)

        // Encontrar o tipo de transporte com o maior número de passageiros
        KStream<String, String> maxTransportTypeStream = passengersByTransportType
                .toStream()
                .map((transportType, passengerCount) -> KeyValue.pair("maxPassengersTransportType",
                        transportType + ":" + passengerCount)) // Adiciona tipo e contagem como valor temporário
                .groupByKey() // Agrupa pela chave fixa "maxTransportType"
                .aggregate(
                        () -> "", // Estado inicial
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
                .filter((key, value) -> !value.isEmpty()); // Filtra valores inválidos

        // Escrever resultado no tópico de saída
        maxTransportTypeStream
                .mapValues(value -> {
                    String[] parts = value.split(":");
                    String transportType = parts[0];

                    // Definir o esquema do JSON
                    String schema = """
                        {
                            "type": "struct",
                            "fields": [
                                {"field": "transportType", "type": "string"}
                            ]
                        }
                    """;

                    // Definir o payload do JSON
                    String payload = String.format(
                            "{\"transportType\": \"%s\"}",
                            transportType
                    );

                    // Retorna o JSON completo com schema e payload
                    return String.format("{\"schema\": %s, \"payload\": %s}", schema, payload);
                })
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String())); // Publica o resultado formatado no tópico de saída

    }
}