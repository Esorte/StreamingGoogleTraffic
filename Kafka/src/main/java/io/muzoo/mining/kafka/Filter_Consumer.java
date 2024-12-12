package io.muzoo.mining.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

public class Filter_Consumer {

    private static final Logger logger = LoggerFactory.getLogger(Filter_Consumer.class);
    private static final String APPLICATION_ID = "traffic-formatter";
    private static final String BOOTSTRAP_SERVERS = "localhost:29092";
    private static final String INPUT_TOPIC = "raw_traffic";
    private static final String OUTPUT_TOPIC = "formatted_traffic";

    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("HH:mm dd/MM/yy");

    public static void main(String[] args) {
        Properties props = createStreamProperties();
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> sourceStream = builder.stream(INPUT_TOPIC);

        KStream<String, String> formattedStream = sourceStream.mapValues(Filter_Consumer::formatData);
        formattedStream.filter((key, value) -> value != null).to(OUTPUT_TOPIC);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        startStream(streams);
    }

    private static Properties createStreamProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        return props;
    }


    private static String formatData(String rawData) {
        try {
            // Extract key-value pairs manually
            String origin = extractValue(rawData, "Origin: ", ", Destination:");
            if (origin == null) {
                origin = "Unknown";
                logger.warn("Missing 'Origin' field in data: {}", rawData);
            }

            String destination = extractValue(rawData, "Destination: ", ", Data:");
            if (destination == null) {
                destination = "Unknown";
                logger.warn("Missing 'Destination' field in data: {}", rawData);
            }

            String distance = extractValue(rawData, "\"distance\" : { \"text\" : \"", "\", \"value\"");
            if (distance == null) {
                distance = "N/A";
                logger.warn("Missing 'Distance' field in data: {}", rawData);
            }

            String durationText = extractValue(rawData, "\"duration\" : { \"text\" : \"", "\", \"value\"");
            if (durationText == null) {
                durationText = "N/A";
                logger.warn("Missing 'Duration Text' field in data: {}", rawData);
            }

            String durationSeconds = extractValue(rawData, "\"value\" : ", " }");
            if (durationSeconds == null) {
                durationSeconds = "0";
                logger.warn("Missing 'Duration Seconds' field in data: {}", rawData);
            }

            // Calculate ETA
            int durationInSeconds = Integer.parseInt(durationSeconds);
            int hours = durationInSeconds / 3600;
            int minutes = (durationInSeconds % 3600) / 60;
            String eta = String.format("%d:%02d", hours, minutes);

            // Get the current date and time
            String dateTime = LocalDateTime.now().format(dateTimeFormatter);

            // Format the output
            return String.format("Route_ID: 01%nOrigin: %s%nDestination: %s%nDistance: %s%nETA: %s%nDateTime: %s",
                    origin, destination, distance, eta, dateTime);
        } catch (Exception e) {
            logger.error("Error formatting data: {}", rawData, e);
            return null;
        }
    }


    private static String extractValue(String data, String startToken, String endToken) {
        int startIndex = data.indexOf(startToken);
        if (startIndex == -1) {
            logger.warn("Start token '{}' not found in data: {}", startToken, data);
            return null;
        }
        startIndex += startToken.length();

        int endIndex = data.indexOf(endToken, startIndex);
        if (endIndex == -1) {
            logger.warn("End token '{}' not found in data: {}", endToken, data);
            return null;
        }

        return data.substring(startIndex, endIndex).trim();
    }

    private static void startStream(KafkaStreams streams) {
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
