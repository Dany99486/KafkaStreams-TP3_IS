package streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.json.JSONObject;

import java.util.Properties;

public class PassengersPerRoute {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "passengers-per-route-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9093,broker3:9094");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        // Consome mensagens do tópico "Trips_topic"
        KStream<String, String> tripsStream = builder.stream("Trips_topic");

        // Extrai o campo "routeId" do payload
        KTable<String, Long> passengersPerRoute = tripsStream
            .mapValues((String value) -> {  // Adicionando tipo explícito no lambda
                try {
                    JSONObject json = new JSONObject(value);
                    JSONObject payload = json.getJSONObject("payload");
                    return payload.getString("routeId");
                } catch (Exception e) {
                    System.err.println("Erro ao parsear JSON: " + e.getMessage());
                    return null; // Retorna null se houver erro
                }
            })
            .filter((key, routeId) -> routeId != null) // Filtra mensagens inválidas
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