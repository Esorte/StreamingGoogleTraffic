package io.muzoo.mining.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.*;
import java.net.HttpURLConnection;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.net.URL;

public class Producer {

    private static final String KAFKA_BROKER = "localhost:29092";
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

    private static Map<String, double[]> locationMap = new HashMap<>();
    static {
        locationMap.put("MUIC", new double[]{13.792932197901337, 100.32602729651516});
        locationMap.put("Siam Paragon", new double[]{13.74591036679997, 100.5344231541853});
        locationMap.put("Victory Monument", new double[]{13.76522600064536, 100.53818429867663});
        locationMap.put("Suvarnabhumi Airport", new double[]{13.68394085330947, 100.74737181171255});
        // Add more
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ObjectMapper objectMapper = new ObjectMapper();
        Set<String> processedPairs = new HashSet<>();

        while (true) {
            for (Map.Entry<String, double[]> originEntry : locationMap.entrySet()) {
                for (Map.Entry<String, double[]> destinationEntry : locationMap.entrySet()) {
                    if (!originEntry.getKey().equals(destinationEntry.getKey())) {
                        String originName = originEntry.getKey();
                        String destinationName = destinationEntry.getKey();
                        String pairKey = originName + "->" + destinationName;
                        String reversePairKey = destinationName + "->" + originName;

                        if (!processedPairs.contains(pairKey) && !processedPairs.contains(reversePairKey)) {
                            processedPairs.add(pairKey);
                            double[] origin = originEntry.getValue();
                            double[] destination = destinationEntry.getValue();
                            String response = fetchGoogleMapsData(origin, destination);
                            if (response != null) {
                                try {
                                    Map<String, Object> responseMap = objectMapper.readValue(response, Map.class);
                                    String message = String.format("Origin: %s (%f, %f), Destination: %s (%f, %f), Data: %s",
                                            originName, origin[0], origin[1], destinationName, destination[0], destination[1], response);
                                    producer.send(new ProducerRecord<>(TOPIC, message));
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                    }
                }
            }
            try {
                Thread.sleep(6000); // Wait for 1 minute before next request
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static String fetchGoogleMapsData(double[] origin, double[] destination) {
        String originStr = origin[0] + "," + origin[1];
        String destinationStr = destination[0] + "," + destination[1];
        String requestUrl = URL + "?origins=" + originStr + "&destinations=" + destinationStr + "&mode=driving&key=" + API_KEY;

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