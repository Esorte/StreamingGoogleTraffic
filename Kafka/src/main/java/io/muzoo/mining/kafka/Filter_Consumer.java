package io.muzoo.mining.kafka;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.http.HttpHost;
import org.json.JSONObject;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.time.LocalDateTime;
import java.time.format.*;

public class Filter_Consumer {
    private static final Logger logger = LoggerFactory.getLogger(Filter_Consumer.class);
    private static final String INPUT_TOPIC = "raw_traffic";
    private static final String OUTPUT_TOPIC = "output-topic";
    private static final String APPLICATION_ID = "filter-consumer-app";
    private static final String BOOTSTRAP_SERVERS = "localhost:29092";
    private static final Map<String, Integer> routeIdMap = new HashMap<>();
    private static final AtomicInteger nextId = new AtomicInteger(1);

    private static RestHighLevelClient esClient;

    public static void main(String[] args) {
        logger.info("Starting the Kafka Streams application...");

        esClient = createElasticsearchClient();

        Properties props = createStreamProperties();
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> sourceStream = builder.stream(INPUT_TOPIC);
        logger.info("Source stream created from topic: {}", INPUT_TOPIC);

        KStream<String, String> formattedStream = sourceStream.mapValues(Filter_Consumer::formatData);
        logger.info("Formatted stream created.");

        formattedStream.filter((key, value) -> value != null).foreach((key, value) -> {
            indexToElasticsearch(key, value);
        });
        logger.info("Filtered stream sent to Elasticsearch.");

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

            JSONObject jsonData = new JSONObject(rawData.substring(rawData.indexOf("Data: ") + 6));
            double distance = jsonData.getJSONArray("rows")
                                      .getJSONObject(0)
                                      .getJSONArray("elements")
                                      .getJSONObject(0)
                                      .getJSONObject("distance")
                                      .getDouble("value") / 1000.0; // Convert meters to kilometers

            int duration = jsonData.getJSONArray("rows")
                                   .getJSONObject(0)
                                   .getJSONArray("elements")
                                   .getJSONObject(0)
                                   .getJSONObject("duration")
                                   .getInt("value") / 60; // Convert seconds to minutes

            String routeKey = origin + "->" + destination;
            int routeId = routeIdMap.computeIfAbsent(routeKey, k -> nextId.getAndIncrement());

            String datetime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
);

            return String.format("{ \"id\": \"%d\", \"datetime\": \"%s\", \"origin\": \"%s\", \"destination\": \"%s\", \"distance\": %.1f, \"duration\": %d }",
                    routeId, datetime, origin, destination, distance, duration);
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

    private static RestHighLevelClient createElasticsearchClient() {
        RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200, "http"));
        return new RestHighLevelClient(builder);
    }

    private static void indexToElasticsearch(String key, String value) {
        IndexRequest request = new IndexRequest("kafka-data").source(value, XContentType.JSON);
        try {
            esClient.index(request, RequestOptions.DEFAULT);
            logger.info("Indexed data to Elasticsearch: {}", value);
        } catch (IOException e) {
            logger.error("Failed to index data to Elasticsearch: {}", value, e);
        }
    }

    private static void startStream(KafkaStreams streams) {
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}