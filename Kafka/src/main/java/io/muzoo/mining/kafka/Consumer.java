package io.muzoo.mining.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONArray;
import org.json.JSONObject;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer {

    private static final String KAFKA_BROKER = "localhost:29092";
    private static final String TOPIC = "raw_traffic";
    private static final String GROUP_ID = "raw-traffic-group";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                String value = record.value();
                JSONObject jsonObject = new JSONObject(value);

                String origin = jsonObject.getJSONArray("origin_addresses").getString(0);
                String destination = jsonObject.getJSONArray("destination_addresses").getString(0);
                JSONObject element = jsonObject.getJSONArray("rows").getJSONObject(0).getJSONArray("elements").getJSONObject(0);
                String distance = element.getJSONObject("distance").getString("text");
                String duration = element.getJSONObject("duration").getString("text");

                System.out.printf("Origin: %s, Destination: %s, Distance: %s, Duration: %s%n",
                        origin, destination, distance, duration);
            }
        }
    }
}