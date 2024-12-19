package streamFunctions;

import classes.Trip;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import utils.JsonDeserializer;
import utils.JsonSerializer;
import utils.KafkaTopicUtils;

public class AveragePassengersPerTransportType {

    private static final String OUTPUT_TOPIC = "projeto3_average_passengers_per_transport_types";
    private static final String INPUT_TRIPS_TOPIC = "Trips_topic";

    public static void addAveragePassengersPerTransportTypeStream(StreamsBuilder builder, KafkaTopicUtils topicUtils) {

        // Garantir que o tópico de saída existe com 3 partições e fator de replicação 1
        topicUtils.createTopicIfNotExists(OUTPUT_TOPIC, 3, (short) 1);

        // Configura o serializador e deserializador JSON para objetos Trip
        JsonDeserializer<Trip> tripDeserializer = new JsonDeserializer<>(Trip.class);
        JsonSerializer<Trip> tripSerializer = new JsonSerializer<>();

        // Lê o fluxo de entradas do tópico de Trips
        KStream<String, Trip> tripsStream = builder.stream(
                INPUT_TRIPS_TOPIC,
                Consumed.with(Serdes.String(), Serdes.serdeFrom(tripSerializer, tripDeserializer))
        );

        // Conta o total de trips agrupando todas as mensagens em uma única chave "key"
        KTable<String, Long> totalTrips = tripsStream
                .groupBy((key, trip) -> "key") // Todas as mensagens recebem a mesma chave
                .count(); // Conta o número total de mensagens (trips)

        // Conta o total de passageiros por tipo de transporte agrupando pelo campo transportType
        KTable<String, Long> totalPassengersByTransportType = tripsStream
                .groupBy((key, trip) -> trip.getTransportType()) // Agrupa pelo tipo de transporte
                .count();

        // Reparticiona totalTrips para mapear por tipo de transporte
        KStream<String, String> repartitionedTotalTrips = totalPassengersByTransportType
                .toStream()
                .map((key, value) -> {
                    // Mapeia para criar uma chave com formato customizado
                    String newKey = "key";
                    String newValue = key+":"+ value;
                    return new KeyValue<>(newKey, newValue);
                });

        // Fazer média para cada transport type:
        KStream<String, Object> averagePassengersPerTransportType = repartitionedTotalTrips
                .join(totalTrips,
                        (passengerCount, tripCount) ->{
                            String[] parts = passengerCount.split(":");
                            long passengerCountValue = Long.parseLong(parts[1]);
                            System.out.println(  parts[0]+":"+ (double) passengerCountValue / tripCount);
                            return parts[0]+":"+ (double) passengerCountValue / tripCount;
                        })
                .map((key, value) -> new KeyValue<>(value.split(":")[0], Double.parseDouble(value.split(":")[1])));


        // Emite os resultados para o tópico de saída formatados como JSON
        averagePassengersPerTransportType
                .mapValues((key, average) -> {
                    // Define o schema do JSON
                    String schema = """
                    {
                        "type": "struct",
                        "fields": [
                            {"field": "transportType", "type": "string"},
                            {"field": "averagePassengers", "type": "double"}
                        ]
                    }
                    """;

                    // Cria o payload com o tipo de transporte e a média calculada
                    String payload = String.format(
                            "{\"transportType\": \"%s\", \"averagePassengers\": %.2f}",
                            key, average
                    );

                    // Retorna o JSON completo com schema e payload
                    return String.format("{\"schema\": %s, \"payload\": %s}", schema, payload);
                })
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String())); // Produz para o tópico de saída
    }
}
