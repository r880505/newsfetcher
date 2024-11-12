package id.labs247.medan.newsfetcher.repositories;

import java.io.IOException;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import id.labs247.medan.newsfetcher.configs.DatabaseConfig;
import id.labs247.medan.newsfetcher.models.CrawlExtraComment;
import id.labs247.medan.newsfetcher.models.CrawlMedia;

public class CrawlMediaRepository {

    public Connection getConnection() throws SQLException, IOException {
        return DatabaseConfig.getDbConnection();
    }

    public List<CrawlMedia> getAllNewsPortal() throws IOException {
        List<CrawlMedia> crawlMedias = new ArrayList<>();
        try (Connection connection = this.getConnection();
            PreparedStatement statement = connection.prepareStatement("SELECT * FROM crawl_media ")) {
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    CrawlMedia crawlMedia = new CrawlMedia();
                    crawlMedia.setMediaId(resultSet.getLong("media_id"));
                    crawlMedia.setOriginalDomain(resultSet.getString("original_domain"));
                    crawlMedia.setLandingUrl(resultSet.getString("landing_url"));
                    crawlMedia.setLastScheduled(resultSet.getObject("last_scheduled", LocalDateTime.class));
                    crawlMedia.setScheduleMinutes(resultSet.getInt("schedule_minutes"));
                    crawlMedia.setLastUpdate(resultSet.getObject("last_update", LocalDateTime.class));
                    crawlMedia.setStatus(resultSet.getInt("status"));
                    crawlMedia.setMediaLogo(resultSet.getString("media_logo"));
                    crawlMedia.setUrlSelect(resultSet.getString("url_select"));
                    crawlMedia.setContentSelect(resultSet.getString("content_select"));
                    crawlMedia.setParseAuto(resultSet.getInt("parse_auto"));
                    crawlMedia.setMaxDepth(resultSet.getInt("max_depth"));
                    crawlMedia.setBacajugaSelect(resultSet.getString("bacajuga_select"));
                    crawlMedia.setImageSelect(resultSet.getString("image_select"));
                    crawlMedia.setDateFormatId(resultSet.getLong("date_format_id"));
                    crawlMedia.setPageParam(resultSet.getString("page_param") != null ? resultSet.getString("page_param") : "");
                    crawlMedia.setIndexPageCount(resultSet.getInt("index_page_count"));
                    crawlMedia.setExtraStatus(resultSet.getInt("extra_status"));
                    crawlMedia.setExtra(resultSet.getString("extra") != null ? resultSet.getString("extra") : "");
                    crawlMedias.add(crawlMedia);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return crawlMedias;
    }

    public List<CrawlMedia> getAllActiveNewsPortal() throws IOException {
        List<CrawlMedia> crawlMedias = new ArrayList<>();
        try (Connection connection = this.getConnection();
            PreparedStatement statement = connection.prepareStatement("SELECT * FROM crawl_media WHERE schedule_minutes >= 0 ")) {
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    CrawlMedia crawlMedia = new CrawlMedia();
                    crawlMedia.setMediaId(resultSet.getLong("media_id"));
                    crawlMedia.setOriginalDomain(resultSet.getString("original_domain"));
                    crawlMedia.setLandingUrl(resultSet.getString("landing_url"));
                    crawlMedia.setLastScheduled(resultSet.getObject("last_scheduled", LocalDateTime.class));
                    crawlMedia.setScheduleMinutes(resultSet.getInt("schedule_minutes"));
                    crawlMedia.setLastUpdate(resultSet.getObject("last_update", LocalDateTime.class));
                    crawlMedia.setStatus(resultSet.getInt("status"));
                    crawlMedia.setMediaLogo(resultSet.getString("media_logo"));
                    crawlMedia.setUrlSelect(resultSet.getString("url_select"));
                    crawlMedia.setContentSelect(resultSet.getString("content_select"));
                    crawlMedia.setParseAuto(resultSet.getInt("parse_auto"));
                    crawlMedia.setMaxDepth(resultSet.getInt("max_depth"));
                    crawlMedia.setBacajugaSelect(resultSet.getString("bacajuga_select"));
                    crawlMedia.setImageSelect(resultSet.getString("image_select"));
                    crawlMedia.setDateFormatId(resultSet.getLong("date_format_id"));
                    crawlMedia.setPageParam(resultSet.getString("page_param") != null ? resultSet.getString("page_param") : "");
                    crawlMedia.setIndexPageCount(resultSet.getInt("index_page_count"));
                    crawlMedia.setExtraStatus(resultSet.getInt("extra_status"));
                    crawlMedia.setExtra(resultSet.getString("extra") != null ? resultSet.getString("extra") : "");
                    crawlMedias.add(crawlMedia);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return crawlMedias;
    }

    public CrawlMedia getNewsPortalByDomain(String domain) throws IOException {
        CrawlMedia crawlMedia = new CrawlMedia();
        try (Connection connection = this.getConnection();
            PreparedStatement statement = connection.prepareStatement("SELECT * FROM crawl_media WHERE original_domain =?")) {
            statement.setString(1, domain);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    crawlMedia.setMediaId(resultSet.getLong("media_id"));
                    crawlMedia.setOriginalDomain(resultSet.getString("original_domain"));
                    crawlMedia.setLandingUrl(resultSet.getString("landing_url"));
                    crawlMedia.setLastScheduled(resultSet.getObject("last_scheduled", LocalDateTime.class));
                    crawlMedia.setScheduleMinutes(resultSet.getInt("schedule_minutes"));
                    crawlMedia.setLastUpdate(resultSet.getObject("last_update", LocalDateTime.class));
                    crawlMedia.setStatus(resultSet.getInt("status"));
                    crawlMedia.setMediaLogo(resultSet.getString("media_logo"));
                    crawlMedia.setUrlSelect(resultSet.getString("url_select"));
                    crawlMedia.setContentSelect(resultSet.getString("content_select"));
                    crawlMedia.setParseAuto(resultSet.getInt("parse_auto"));
                    crawlMedia.setMaxDepth(resultSet.getInt("max_depth"));
                    crawlMedia.setBacajugaSelect(resultSet.getString("bacajuga_select"));
                    crawlMedia.setImageSelect(resultSet.getString("image_select"));
                    crawlMedia.setDateFormatId(resultSet.getLong("date_format_id"));
                    crawlMedia.setPageParam(resultSet.getString("page_param") != null ? resultSet.getString("page_param") : "");
                    crawlMedia.setIndexPageCount(resultSet.getInt("index_page_count"));
                    crawlMedia.setExtraStatus(resultSet.getInt("extra_status"));
                    crawlMedia.setExtra(resultSet.getString("extra") != null ? resultSet.getString("extra") : "");
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return crawlMedia;
    }

    public List<String> getAllNewsPortalDomain() throws IOException {
        List<String> domains = new ArrayList<>();
        try (Connection connection = this.getConnection();
            PreparedStatement statement = connection.prepareStatement("SELECT original_domain FROM crawl_media");
            ResultSet resultSet = statement.executeQuery()) {
            
            while (resultSet.next()) {
                String domain = resultSet.getString("original_domain");
                domains.add(domain);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new IOException("Error retrieving content filters from database", e);
        }
        return domains;
    }

    public void update(CrawlMedia crawlMedia) throws IOException {
        String sql = "UPDATE crawl_media SET " +
                     "original_domain = ?, " +
                     "landing_url = ?, " +
                     "last_scheduled = ?, " +
                     "schedule_minutes = ?, " +
                     "last_update = ?, " +
                     "status = ?, " +
                     "media_logo = ?, " +
                     "url_select = ?, " +
                     "content_select = ?, " +
                     "parse_auto = ?, " +
                     "max_depth = ?, " +
                     "bacajuga_select = ?, " +
                     "image_select = ?, " +
                     "date_format_id = ?, " +
                     "page_param = ?, " +
                     "index_page_count = ?, " +
                     "extra_status = ?, " +
                     "extra = ? " +
                     "WHERE media_id = ?";
        try (Connection connection = this.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            preparedStatement.setString(1, crawlMedia.getOriginalDomain());
            preparedStatement.setString(2, crawlMedia.getLandingUrl());
            preparedStatement.setString(3, crawlMedia.getLastScheduled() != null ? crawlMedia.getLastScheduled().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) : null);
            preparedStatement.setInt(4, crawlMedia.getScheduleMinutes());
            preparedStatement.setString(5, crawlMedia.getLastUpdate() != null ? crawlMedia.getLastUpdate().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) : null);
            preparedStatement.setInt(6, crawlMedia.getStatus());
            preparedStatement.setString(7, crawlMedia.getMediaLogo());
            preparedStatement.setString(8, crawlMedia.getUrlSelect());
            preparedStatement.setString(9, crawlMedia.getContentSelect());
            preparedStatement.setInt(10, crawlMedia.getParseAuto());
            preparedStatement.setInt(11, crawlMedia.getMaxDepth());
            preparedStatement.setString(12, crawlMedia.getBacajugaSelect());
            preparedStatement.setString(13, crawlMedia.getImageSelect());
            preparedStatement.setLong(14, crawlMedia.getDateFormatId());
            preparedStatement.setString(15, crawlMedia.getPageParam() != null ? crawlMedia.getPageParam() : null); 
            preparedStatement.setInt(16, crawlMedia.getIndexPageCount());
            preparedStatement.setInt(17, crawlMedia.getExtraStatus());
            preparedStatement.setString(18, crawlMedia.getExtra());
            preparedStatement.setLong(19, crawlMedia.getMediaId());

            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public List<String> getBackdateNewsPortal() throws IOException {
        List<String> domains = new ArrayList<>();
        String sql = "SELECT cm.original_domain "
                        + "FROM crawl_media cm "
                        + "JOIN url_format uf ON cm.media_id = uf.media_id "
                        + "JOIN date_format df ON cm.date_format_id = df.id "
                        + "WHERE cm.schedule_minutes >= 0 "
                        + "AND uf.format LIKE '%date%' "
                        + "AND df.format LIKE '%yyyy%' "
                        + "AND df.format LIKE '%MM%' "
                        + "AND df.format LIKE '%dd%'";
        try (Connection connection = this.getConnection();
            PreparedStatement statement = connection.prepareStatement(sql);
            ResultSet resultSet = statement.executeQuery()) {
            
            while (resultSet.next()) {
                String domain = resultSet.getString("original_domain");
                domains.add(domain);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new IOException("Error retrieving content filters from database", e);
        }
        return domains;
    }

    public CrawlExtraComment getCrawlExtraCommentByMediaId(Long mediaId)  throws IOException {
        CrawlExtraComment crawlExtraComment = new CrawlExtraComment();
        try (Connection connection = this.getConnection();
            PreparedStatement statement = connection.prepareStatement("SELECT * FROM crawl_extra_comment WHERE media_id =? ")) {
            statement.setLong(1, mediaId);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    crawlExtraComment.setId(resultSet.getLong("id"));
                    crawlExtraComment.setCommentApi(resultSet.getString("comment_api"));
                    crawlExtraComment.setArticleIdSelect(resultSet.getString("article_id_select"));
                    crawlExtraComment.setRequestMethod(resultSet.getString("request_method"));
                    crawlExtraComment.setRequestBody(resultSet.getString("request_body") != null ? resultSet.getString("request_body") : "");
                    crawlExtraComment.setRequestParam(resultSet.getString("request_param") != null ? resultSet.getString("request_param") : "");
                    crawlExtraComment.setMediaId(resultSet.getLong("media_id"));
                    crawlExtraComment.setCookie(resultSet.getString("cookie") != null ? resultSet.getString("cookie") : "");
                    crawlExtraComment.setSelector(resultSet.getString("selector"));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return crawlExtraComment;
    }

}
