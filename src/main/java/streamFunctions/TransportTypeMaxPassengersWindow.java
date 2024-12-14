package streamFunctions;

import classes.Trip;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import utils.JsonDeserializer;
import utils.JsonSerializer;
import utils.KafkaTopicUtils;

import java.time.Duration;

public class TransportTypeMaxPassengersWindow {

    private static final String INPUT_TRIPS_TOPIC = "Trips_topic";
    private static final String OUTPUT_TOPIC = "projeto3_max_transport_type_window";

    /**
     * Este método adiciona a lógica de processamento ao StreamsBuilder fornecido.
     * A ideia é que você possa chamar este método a partir de outro arquivo,
     * passando o builder (compartilhado entre várias topologias) e um objeto KafkaTopicUtils
     * para garantir que o tópico de saída existe, etc.
     *
     * Após chamar este método, a topologia estará configurada. Você poderá então
     * criar e iniciar a sua instância de KafkaStreams externamente.
     *
     * @param builder    StreamsBuilder ao qual a topologia será adicionada
     * @param topicUtils Instância de KafkaTopicUtils para gerenciamento de tópicos
     */
    public static void addTransportTypeMaxPassengersWindowStream(StreamsBuilder builder, KafkaTopicUtils topicUtils) {
        topicUtils.createTopicIfNotExists(OUTPUT_TOPIC, 3, (short) 1);

        // Serde para Trip
        var tripSerde = Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(Trip.class));

        // Consome o tópico de trips
        KStream<String, Trip> tripsStream = builder.stream(
                INPUT_TRIPS_TOPIC,
                Consumed.with(Serdes.String(), tripSerde)
        );

        // Contagem de viagens por tipo de transporte em uma janela de 1 minuto
        KTable<Windowed<String>, Long> passengersByTransportType = tripsStream
                .filter((key, trip) -> trip != null && trip.getTransportType() != null) // Filtra valores nulos
                .groupBy((key, trip) -> trip.getTransportType(),
                        Grouped.with(Serdes.String(), tripSerde))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
                .count(Materialized.with(Serdes.String(), Serdes.Long()));

        // Aqui convertemos em stream e mapeamos todos os resultados para uma mesma chave fixa ("maxPassengers")
        // Assim, todos os valores ficarão armazenados em apenas um registro agregado, que será atualizado conforme chegam novos valores.
        KStream<String, String> maxTransportTypeStream = passengersByTransportType
                .toStream()
                .mapValues((windowedKey, passengerCount) -> {
                    // Valor no formato "transportType:passengerCount"
                    return windowedKey.key() + ":" + passengerCount;
                })
                // Agora reagrupamos com chave fixa "maxPassengers"
                .selectKey((windowedKey, value) -> "maxPassengers")
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .aggregate(
                        () -> "", // Estado inicial vazio
                        (key, newValue, currentMax) -> {
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
                .mapValues(value -> {
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

                    return String.format("{\"schema\": %s, \"payload\": %s}", schema, payload);
                });

        // Publicar o resultado no tópico de saída
        maxTransportTypeStream.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
    }
}