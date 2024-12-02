package streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.KeyValue;
import org.json.JSONObject;

import java.util.Properties;

public class TotalOccupancyPercentage {

    private static final String OUTPUT_TOPIC = "projeto3_total_occupancy_percentage";
    private static final String INPUT_ROUTES_TOPIC = "Routes_topic";
    private static final String INPUT_TRIPS_TOPIC = "Trips_topic";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "total-occupancy-percentage-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9093,broker3:9094");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        //Capacidades das rotas
        KStream<String, String> routesStream = builder.stream(INPUT_ROUTES_TOPIC);

        KTable<String, Integer> routeCapacities = routesStream
                .mapValues(value -> {
                    try {
                        JSONObject json = new JSONObject(value);
                        JSONObject payload = json.getJSONObject("payload");
                        return payload.getInt("capacity");
                    } catch (Exception e) {
                        System.err.println("Erro ao parsear JSON: " + e.getMessage());
                        return null;
                    }
                })
                .filter((routeId, capacity) -> capacity != null) // Filtrar mensagens inválidas
                .groupByKey()
                .aggregate(
                        () -> 0,
                        (routeId, newCapacity, currentCapacity) -> newCapacity,
                        Materialized.with(Serdes.String(), Serdes.Integer())
                );

        //Contar passageiros
        KStream<String, String> tripsStream = builder.stream(INPUT_TRIPS_TOPIC);

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
                .filter((key, routeId) -> routeId != null) // Filtrar mensagens inválidas
                .groupBy((key, routeId) -> routeId)
                .count(Materialized.with(Serdes.String(), Serdes.Long()));

        //Capacidade total
        KTable<String, Integer> totalCapacity = routeCapacities
            .groupBy(
                (routeId, capacity) -> KeyValue.pair("total", capacity), //Mapeia todas as rotas para a chave "total"
                Grouped.with(Serdes.String(), Serdes.Integer()) 
            )
            .reduce(
                Integer::sum,
                Integer::sum,
                Materialized.with(Serdes.String(), Serdes.Integer())
            );
        
        //Total de passageiros
        KTable<String, Long> totalPassengers = passengersPerRoute
            .groupBy(
                (routeId, passengers) -> KeyValue.pair("total", passengers), //Mapeia todas as rotas para a chave "total"
                Grouped.with(Serdes.String(), Serdes.Long())               
            )
            .reduce(
                Long::sum,
                Long::sum,
                Materialized.with(Serdes.String(), Serdes.Long())
            );

        //Calcular a percentagem de ocupacao
        KTable<String, Double> totalOccupancyPercentage = totalCapacity
                .join(
                        totalPassengers,
                        (capacity, passengers) -> {
                            if (capacity == 0 || passengers == null) return 0.0; //Evitar divisão por zero
                            return (passengers.doubleValue() / capacity) * 100;
                        },
                        Materialized.with(Serdes.String(), Serdes.Double())
                );

        //Escrever o resultado no topico
        totalOccupancyPercentage.toStream()
                .filter((key, value) -> key.equals("total")) //Apenas a chave "total"
                .mapValues(percentage -> {
                    String schema = """
                    {
                        "type": "struct",
                        "fields": [
                            {"field": "totalOccupancyPercentage", "type": "double"}
                        ]
                    }
                    """;

                    String payload = String.format("{\"totalOccupancyPercentage\": %.2f}", percentage);

                    return String.format("{\"schema\": %s, \"payload\": %s}", schema, payload);
                })
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));


        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}