package streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.json.JSONObject;

import java.util.Properties;

public class AvailableSeatsPerRoute {

    public static void main(String[] args) {
        
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "available-seats-per-route-app1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9093,broker3:9094");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        //Processamento do tópico Routes_topic
        KStream<String, String> routesStream = builder.stream("Routes_topic");

        KTable<String, Integer> routeCapacities = routesStream
            .mapValues(value -> {
                try {
                    JSONObject json = new JSONObject(value);
                    JSONObject payload = json.getJSONObject("payload");
                    String routeId = payload.getString("routeId");
                    int capacity = payload.getInt("capacity");
                    return routeId + ":" + capacity;
                } catch (Exception e) {
                    System.err.println("Erro ao parsear JSON: " + e.getMessage());
                    return null;
                }
            })
            .filter((key, value) -> value != null) //Filtra mensagens inválidas
            .groupBy((key, value) -> value.split(":")[0]) //Agrupa pelo routeId
            .aggregate(
                () -> 0,
                (key, value, aggregate) -> Integer.parseInt(value.split(":")[1]),
                Materialized.with(Serdes.String(), Serdes.Integer())
            );

        // Processamento do tópico Trips_topic
        KStream<String, String> tripsStream = builder.stream("Trips_topic");

        KTable<String, Long> passengersPerRoute = tripsStream
            .mapValues(value -> {
                try {
                    JSONObject json = new JSONObject(value);
                    JSONObject payload = json.getJSONObject("payload");
                    return payload.getString("routeId");
                } catch (Exception e) {
                    System.err.println("Erro ao parsear JSON: " + e.getMessage());
                    return null;
                }
            })
            .filter((key, routeId) -> routeId != null) //Filtra mensagens inválidas
            .groupBy((key, routeId) -> routeId) //Agrupa por routeId
            .count(Materialized.with(Serdes.String(), Serdes.Long()));

        // Cálculo dos assentos disponíveis por rota
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