package id.labs247.medan.newsfetcher.repositories;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import id.labs247.medan.newsfetcher.configs.DatabaseConfig;

public class UrlFilterRepository {

    public Connection getConnection() throws SQLException, IOException {
        return DatabaseConfig.getDbConnection();
    }

    public List<String> getAllUrlFilter() throws IOException {
        List<String> filters = new ArrayList<>();
        try (Connection connection = this.getConnection();
             PreparedStatement statement = connection.prepareStatement("SELECT filter FROM url_filter ");
             ResultSet resultSet = statement.executeQuery()) {
            
            while (resultSet.next()) {
                String filter = resultSet.getString("filter");
                filters.add(filter);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return filters;
    }
    
}
