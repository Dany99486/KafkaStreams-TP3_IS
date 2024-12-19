import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import streamFunctions.*;
import utils.KafkaTopicUtils;

import java.util.Properties;

public class MainStreamApp {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "consolidated-stream-app74");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9093,broker3:9094");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        KafkaTopicUtils topicUtils = new KafkaTopicUtils(props);

        StreamsBuilder builder = new StreamsBuilder();

        AvailableSeatsPerRoute.addAvailableSeatsPerRouteStream(builder, topicUtils);
        AveragePassengersPerTransportType.addAveragePassengersPerTransportTypeStream(builder, topicUtils);
        NamePassengerMostTrips.addNamePassengerMostTripsStreams(builder, topicUtils);
        OccupancyPerRoute.addOccupancyPerRouteStream(builder, topicUtils);
        PassengersPerRoute.addPassengersPerRouteStream(builder, topicUtils);
        RouteWithLeastOccupancyPerTransportType.addRouteWithLeastOccupancyPerTransportTypeStream(builder, topicUtils);
        TotalCapacityAvailable.addTotalCapacityAvailableStream(builder,topicUtils);
        TotalOccupancyPercentage.addTotalOccupancyPercentageStream(builder, topicUtils);
        TotalPassengers.addTotalPassengersStream(builder, topicUtils);
        TransportTypeMaxPassengers.addTransportTypeMaxPassengersStream(builder, topicUtils);
        TransportTypeMaxPassengersWindow.addTransportTypeMaxPassengersWindowStream(builder, topicUtils); //Ponto 13
        LeastOccupiedTransportTypeWindow.addLeastOccupiedTransportTypeWindowStream(builder, topicUtils); //Ponto 14
        MostOccupiedOperator.addMostOccupiedOperatorStream(builder, topicUtils); //Ponto 15


        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();


        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            topicUtils.close();
        }));
    }
}
