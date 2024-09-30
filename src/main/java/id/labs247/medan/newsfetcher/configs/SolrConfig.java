package id.labs247.medan.newsfetcher.configs;

public class SolrConfig {

    public static String getSolrHost() {
        return ConfigurationLoader.getString("solr.host");
    }

    public static Boolean getSolrIsSecure() {
        return ConfigurationLoader.getBoolean("solr.is-secure");
    }

    public static Boolean getSolrIsCluster() {
        return ConfigurationLoader.getBoolean("solr.is-cluster");
    }

    public static String getSolrZkHosts() {
        return ConfigurationLoader.getString("solr.zk-host");
    }

    public static String getSolrCollection() {
        return ConfigurationLoader.getString("solr.collection");
    }

    public static String getJaasPath() {
        return ConfigurationLoader.getString("jaas.path");
    }

}
