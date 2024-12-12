import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Properties;
import java.util.UUID;

public class Filter_Consumer {

    private static final Logger logger = LoggerFactory.getLogger(Filter_Consumer.class);
    private static final String APPLICATION_ID = "traffic-formatter";
    private static final String BOOTSTRAP_SERVERS = "localhost:29092";
    private static final String INPUT_TOPIC = "raw_traffic";
    private static final String OUTPUT_TOPIC = "formatted_traffic";

    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("HH:mm dd/MM/yy");
    private static final HashMap<String, String> routeIds = new HashMap<>();

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
            // Extract required values
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

            return String.format("{ \"origin\": \"%s\", \"destination\": \"%s\", \"distance\": %.1f, \"duration\": %d }",
                    origin, destination, distance, duration);
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
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}