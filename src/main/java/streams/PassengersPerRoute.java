package streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

public class PassengersPerRoute {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "passengers-per-route-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9093,broker3:9094");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> tripsStream = builder.stream("Trips_topic");

        KTable<String, Long> passengersPerRoute = tripsStream
            .mapValues(value -> {
                //Parseia do JSON para extrair o routeId
                //Assumindo que o valor seja algo como {"tripId":"Trip_1", "routeId":"Route_1", "origin":"Origin_X", ...}
                String routeId = value.split("\"routeId\":\"")[1].split("\"")[0];
                return routeId;
            })
            .groupBy((key, routeId) -> routeId)
            .count();

        passengersPerRoute.toStream().foreach((routeId, count) -> 
            System.out.printf("Route ID: %s, Passengers: %d%n", routeId, count)
        );

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}