package id.labs247.medan.newsfetcher.scraper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import org.apache.logging.log4j.*;
import org.json.JSONObject;

import id.labs247.medan.newsfetcher.configs.KafkaConfig;
import id.labs247.medan.newsfetcher.configs.SolrConfig;


public class KafkaService {
    
    Producer<String, String> kafkaProducer;
    
    private static final long TIMEOUT_MILLIS = 10000;

    private static final Logger logger = LogManager.getLogger(KafkaService.class);

    private static final String kafkaServiceName = "kafka";

    private Boolean isSecure;

    public String getKafkaServer() {
        return KafkaConfig.getKafkaServers();
    }

    public String getGroupId() {
        return KafkaConfig.getGroupId();
    }

    public Boolean getIsSecure() {
        return SolrConfig.getSolrIsSecure();
    }

    public void sendToKafka(String object, String topic) throws Exception {

        String kafkaServer = this.getKafkaServer();
        isSecure = getIsSecure();

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);//"brj2prdsmlwrk1dbs.solusi247.com:6667,brj2prdsmlwrk2dbs.solusi247.com:6667,brj2prdsmlwrk3dbs.solusi247.com:6667");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000); // Increased timeout
        properties.put(ProducerConfig.RETRIES_CONFIG, 3); // Retry configuration
        properties.put("acks", "all");
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);

        if(isSecure) {
            properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            properties.put("sasl.kerberos.service.name", kafkaServiceName);
        }

        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties)) {
            kafkaProducer.send(new ProducerRecord<>(topic, object)).get(); // Wait for send to complete
            logger.info("[DEBUG] Kafka | Successfully sent to Kafka | " + getUrl(object));
        } catch (Exception e) {
            logger.error("[ERROR] Kafka | Failed send to Kafka | " + e.getMessage(), e);
            throw e;
        }
    }

    public List<ConsumerRecord<String, String>> subscribeFromKafka(String topic) throws Exception {

        String kafkaServer = this.getKafkaServer();
        String groupId = getGroupId();
        isSecure = getIsSecure();

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer); //"brj2prdsmlwrk1dbs.solusi247.com:6667,brj2prdsmlwrk2dbs.solusi247.com:6667,brj2prdsmlwrk3dbs.solusi247.com:6667");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 60000);
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000); // Increased timeout
        properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000); // Heartbeat interval
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put("partition.assignment.strategy", "com.github.grantneale.kafka.LagBasedPartitionAssignor");

        if(isSecure) {
            properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            properties.put("sasl.kerberos.service.name", kafkaServiceName);
        } 

        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Arrays.asList(topic));
            long endTimeMillis = System.currentTimeMillis() + TIMEOUT_MILLIS;

            while (System.currentTimeMillis() < endTimeMillis) {
                ConsumerRecords<String, String> records = consumer.poll(500);
                for (ConsumerRecord<String, String> record : records) {
                    buffer.add(record);
                }
            }
        } catch (Exception e) {
            logger.error("[ERROR] Kafka | Failed to consume from Kafka | " + e.getMessage(), e);
        }

        return buffer;
    }
    
    public List<String> parsingKafka(List<ConsumerRecord<String, String>> records) {
        List<String> result = new ArrayList<>();

        for(ConsumerRecord<String, String> value : records) {
            result.add(value.value());
        }
        return result;
    }

    public void close() {
        // Close Kafka
        kafkaProducer.close();
    }

    public String getUrl(String json) {
        return (new JSONObject(json)).getString("url");
    }

}
