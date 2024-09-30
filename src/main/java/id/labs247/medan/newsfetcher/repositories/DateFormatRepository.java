package id.labs247.medan.newsfetcher.repositories;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import id.labs247.medan.newsfetcher.configs.DatabaseConfig;

public class DateFormatRepository {
    
    public Connection getConnection() throws SQLException, IOException {
        return DatabaseConfig.getDbConnection();
    }

    public String getDateFormatById(Long id) throws IOException {
        String dateFormat = "";
        try (Connection connection = this.getConnection();
            PreparedStatement statement = connection.prepareStatement("SELECT format FROM date_format WHERE id =?")) {
                statement.setLong(1, id);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    dateFormat = resultSet.getString("format");
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return dateFormat;
    }
    
}
