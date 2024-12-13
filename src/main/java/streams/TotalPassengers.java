package streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import utils.JsonDeserializer;
import utils.JsonSerializer;
import utils.KafkaTopicUtils;
import classes.Trip;

public class TotalPassengers {

    private static final String OUTPUT_TOPIC = "projeto3_total_passengers";
    private static final String INPUT_TRIPS_TOPIC = "Trips_topic";

    public static void addTotalPassengersStream(StreamsBuilder builder, KafkaTopicUtils topicUtils) {

        topicUtils.createTopicIfNotExists(OUTPUT_TOPIC, 3, (short) 1);

        // Usa JsonSerializer e JsonDeserializer para Trip
        JsonDeserializer<Trip> tripDeserializer = new JsonDeserializer<>(Trip.class);
        JsonSerializer<Trip> tripSerializer = new JsonSerializer<>();

        // Configura o stream com JsonSerializer e JsonDeserializer
        KStream<String, Trip> tripsStream = builder.stream(
                INPUT_TRIPS_TOPIC,
                Consumed.with(Serdes.String(), Serdes.serdeFrom(tripSerializer, tripDeserializer))
        );

        // Processar trips para calcular o total de passageiros
        KTable<String, Long> totalPassengers = tripsStream
                .groupBy((key, trip) -> "totalPassengers") // Agrupa todas as mensagens em uma única chave
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

    }
}