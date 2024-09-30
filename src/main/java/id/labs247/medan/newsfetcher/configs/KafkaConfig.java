package id.labs247.medan.newsfetcher.configs;

public class KafkaConfig {

    public static String getKafkaServers() {
        return ConfigurationLoader.getString("kafka.bootstrap.servers");
    }

    public static String getGroupId() {
        return ConfigurationLoader.getString("kafka.consumer.group-id");
    }

}

