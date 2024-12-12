package io.muzoo.mining.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class producer {

    private static final String KAFKA_BROKER = "localhost:9092";
    private static final String TOPIC = "raw_traffic";
    private static final String API_KEY = getApiKey();
    private static final String URL = "https://maps.googleapis.com/maps/api/distancematrix/json";

    private static String getApiKey() {
        try {
            return new String(Files.readAllBytes(Paths.get("apikey.txt"))).trim();
        } catch (IOException e) {
            throw new RuntimeException("Failed to read API key", e);
        }
    }

    private static Map<double[], double[]> locationMap = new HashMap<>();
    static {
        locationMap.put(new double[]{13.792932197901337, 100.32602729651516}, new double[]{13.74591036679997, 100.5344231541853});  // MUIC -> Siam Paragon
        locationMap.put(new double[]{13.76522600064536, 100.53818429867663}, new double[]{13.68394085330947, 100.74737181171255});  // Victory Monument -> Suvarnabhumi Airport
        // Add more
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ObjectMapper objectMapper = new ObjectMapper();

        while (true) {
            for (Map.Entry<double[], double[]> entry : locationMap.entrySet()) {
                double[] origin = entry.getKey();
                double[] destination = entry.getValue();
                System.out.println("Fetching data for " + origin[0] + "," + origin[1] + " -> " + destination[0] + "," + destination[1]);

                String data = fetchGoogleMapsData(origin, destination);
                if (data != null) {
                    try {
                        Map<String, Object> responseMap = objectMapper.readValue(data, Map.class);
                        for (Map<String, Object> route : (Iterable<Map<String, Object>>) responseMap.get("routes")) {
                            Map<String, Object> processedData = new HashMap<>();
                            processedData.put("origin", Map.of("lat", origin[0], "lng", origin[1]));
                            processedData.put("destination", Map.of("lat", destination[0], "lng", destination[1]));
                            processedData.put("distance", ((Map<String, Object>) ((Map<String, Object>) route.get("legs")).get(0)).get("distance"));
                            processedData.put("duration", ((Map<String, Object>) ((Map<String, Object>) route.get("legs")).get(0)).get("duration"));
                            processedData.put("timestamp", System.currentTimeMillis() / 1000L);

                            String jsonData = objectMapper.writeValueAsString(processedData);
                            System.out.println("Sending data to Kafka: " + jsonData);

                            producer.send(new ProducerRecord<>(TOPIC, jsonData), new Callback() {
                                @Override
                                public void onCompletion(RecordMetadata metadata, Exception exception) {
                                    if (exception == null) {
                                        System.out.println("Data sent successfully: " + metadata.toString());
                                    } else {
                                        System.err.println("Failed to send data to Kafka: " + exception.getMessage());
                                    }
                                }
                            });
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            System.out.println("Sleeping for 60 seconds...");
            try {
                Thread.sleep(60000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static String fetchGoogleMapsData(double[] origin, double[] destination) {
        String originStr = origin[0] + "," + origin[1];
        String destinationStr = destination[0] + "," + destination[1];
        String requestUrl = URL + "?origin=" + originStr + "&destination=" + destinationStr + "&mode=driving&key=" + API_KEY;

        try {
            URL url = new URL(requestUrl);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");

            int responseCode = connection.getResponseCode();
            if (responseCode == 200) {
                BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                String inputLine;
                StringBuilder response = new StringBuilder();

                while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
                }
                in.close();
                return response.toString();
            } else {
                BufferedReader in = new BufferedReader(new InputStreamReader(connection.getErrorStream()));
                String inputLine;
                StringBuilder response = new StringBuilder();

                while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
                }
                in.close();
                System.err.println("Error fetching data for " + originStr + " -> " + destinationStr + ": " + responseCode + ", " + response.toString());
                return null;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
