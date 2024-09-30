package id.labs247.medan.newsfetcher.configs;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public class ConfigurationLoader {
    
    private static CompositeConfiguration configuration = new CompositeConfiguration();

    static {
        try {
            // Load properties
            configuration.addConfiguration(new PropertiesConfiguration("application.properties"));
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
    }

    public static String getString(String key) {
        return configuration.getString(key);
    }

    public static Boolean getBoolean(String key) {
        return configuration.getBoolean(key);
    }

    public static Integer getInteger(String key) {
        return configuration.getInt(key);
    }

    public static Integer getPort() {
        return getInteger("application.port");
    }

    public static String getUrlCheckerApi() {
        return configuration.getString("api.url-checker");
    }

}
