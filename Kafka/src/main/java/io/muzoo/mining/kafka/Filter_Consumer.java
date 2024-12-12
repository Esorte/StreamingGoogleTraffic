import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Filter_Consumer {
    private static final Logger logger = LoggerFactory.getLogger(Filter_Consumer.class);
    private static final String INPUT_TOPIC = "input-topic";
    private static final String OUTPUT_TOPIC = "output-topic";
    private static final String APPLICATION_ID = "filter-consumer-app";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) {
        logger.info("Starting the Kafka Streams application...");

        Properties props = createStreamProperties();
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> sourceStream = builder.stream(INPUT_TOPIC);
        logger.info("Source stream created from topic: {}", INPUT_TOPIC);

        KStream<String, String> formattedStream = sourceStream.mapValues(Filter_Consumer::formatData);
        logger.info("Formatted stream created.");

        formattedStream.filter((key, value) -> value != null).to(OUTPUT_TOPIC);
        logger.info("Filtered stream sent to topic: {}", OUTPUT_TOPIC);

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
            logger.info("Formatting data: {}", rawData);

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

            String distanceText = extractValue(rawData, "\"distance\" : { \"text\" : \"", "\", \"value\"");
            double distance = distanceText != null ? Double.parseDouble(distanceText.split(" ")[0]) : 0.0;

            String durationText = extractValue(rawData, "\"duration\" : { \"text\" : \"", "\", \"value\"");
            int duration = durationText != null ? Integer.parseInt(durationText.split(" ")[0]) : 0;

            String formattedData = String.format("{ \"origin\": \"%s\", \"destination\": \"%s\", \"distance\": %.1f, \"duration\": %d }",
                    origin, destination, distance, duration);
            logger.info("Formatted data: {}", formattedData);
            return formattedData;
        } catch (Exception e) {
            logger.error("Error formatting data: {}", rawData, e);
            return null;
        }
    }

    private static String extractValue(String data, String startDelimiter, String endDelimiter) {
        int startIndex = data.indexOf(startDelimiter);
        if (startIndex == -1) {
            return null;
        }
        startIndex += startDelimiter.length();
        int endIndex = data.indexOf(endDelimiter, startIndex);
        if (endIndex == -1) {
            return null;
        }
        return data.substring(startIndex, endIndex).trim();
    }

    private static void startStream(KafkaStreams streams) {
        streams.start();
        logger.info("Kafka Streams started.");
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}