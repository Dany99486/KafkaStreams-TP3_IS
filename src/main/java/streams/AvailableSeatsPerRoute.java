package streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.KGroupedStream;

import java.util.Properties;

public class AvailableSeatsPerRoute {

    public static void main(String[] args) {
        // Configuração do Kafka Streams
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "available-seats-per-route-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9093,broker3:9094");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> routesStream = builder.stream("Routes_topic");

        KTable<String, Integer> routeCapacities = routesStream
            .mapValues(value -> {
                //Parseia o JSON para extrair o routeId e capacidade
                String routeId = value.split("\"routeId\":\"")[1].split("\"")[0];
                int capacity = Integer.parseInt(value.split("\"capacity\":")[1].split(",")[0]);
                return routeId + ":" + capacity;
            })
            .groupBy((key, value) -> value.split(":")[0]) //Agrupa pelo routeId
            .aggregate(
                () -> 0,
                (key, value, aggregate) -> Integer.parseInt(value.split(":")[1]),
                Materialized.with(Serdes.String(), Serdes.Integer())
            );

        

        KStream<String, String> tripsStream = builder.stream("Trips_topic");

        KTable<String, Long> passengersPerRoute = tripsStream
            .mapValues(value -> {
                String routeId = value.split("\"routeId\":\"")[1].split("\"")[0]; // Extração de routeId como String
                return routeId;
            })
            .groupBy((key, value) -> value) // Agrupa por routeId
            .count(Materialized.with(Serdes.String(), Serdes.Long()));

        //Calcula os assentos disponiveis para cada rota
        KTable<String, Integer> availableSeatsPerRoute = routeCapacities.leftJoin(
            passengersPerRoute,
            (capacity, passengers) -> capacity - (passengers != null ? passengers.intValue() : 0)
        );
        

        availableSeatsPerRoute.toStream().foreach((routeId, availableSeats) ->
            System.out.printf("Route ID: %s, Available Seats: %d%n", routeId, availableSeats)
        );

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}