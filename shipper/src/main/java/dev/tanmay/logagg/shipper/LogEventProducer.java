package dev.tanmay.logagg.shipper;

import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dev.tanmay.logagg.model.LogEvent;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

public class LogEventProducer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(LogEventProducer.class);

    private final Producer<String, LogEvent> producer;
    private final String topic;

    public LogEventProducer(String bootstrapServers, String schemaRegistryUrl, String topic) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "shipper");

        this.producer = new KafkaProducer<>(props);
        this.topic = topic;
    }

    public void send(LogEvent event) {
        String key = event.getService().toString();
        ProducerRecord<String, LogEvent> record = new ProducerRecord<>(topic, key, event);

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("Failed to send event for service={}", key, exception);
            } else {
                log.debug("Sent to {}-{} at offset {}",
                        metadata.topic(), metadata.partition(), metadata.offset());
            }
        });
    }

    @Override
    public void close() {
        log.info("Flushing and closing producer");
        producer.flush();
        producer.close(Duration.ofSeconds(10));
    }
}