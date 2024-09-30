package id.labs247.medan.newsfetcher.configs;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DatabaseConfig {
    
    // Method to establish a connection to the database
    public static Connection getDbConnection() throws SQLException {

        // Retrieve JDBC connection properties
        String url = ConfigurationLoader.getString("datasource.url");
        String username = ConfigurationLoader.getString("datasource.username");
        String password = ConfigurationLoader.getString("datasource.password");

        // Establish connection
        return DriverManager.getConnection(url, username, password);
    }

    // Method to close the connection
    public static void closeConnection(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}

