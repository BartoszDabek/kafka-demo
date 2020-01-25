package pl.bdabek;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class SystemInfoProducer {

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ACKS_CONFIG, "all");
        props.put(LINGER_MS_CONFIG, 1);
        props.put(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        SystemInformation systemInformation = new SystemInformation();
        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < 100000; i++) {
                producer.send(new ProducerRecord<>("cpu", systemInformation.getSerialNumber(), String.valueOf(systemInformation.getCpuUsage())));
                producer.send(new ProducerRecord<>("ram", systemInformation.getSerialNumber(), String.valueOf(systemInformation.getMemoryUsage())));
                Thread.sleep(1500);
            }
        }
    }

}
