package id.labs247.medan.newsfetcher.scraper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import okhttp3.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import id.labs247.medan.newsfetcher.repositories.CrawlMediaRepository;
import id.labs247.medan.newsfetcher.repositories.FilterRepository;
import id.labs247.medan.newsfetcher.models.CrawlMedia;

public class KumparanScraper {

    private static final Logger logger = LogManager.getLogger(KumparanScraper.class);
    private final KafkaService kafkaService = new KafkaService();
    private final CrawlMediaRepository crawlMediaRepository = new CrawlMediaRepository();
    private final FilterRepository filterRepository = new FilterRepository();

    public void parseIndexPage() throws IOException, Exception {
        String domain = "kumparan.com";
        CrawlMedia crawlMedia = crawlMediaRepository.getNewsPortalByDomain(domain);
        Integer maxDepth = crawlMedia.getMaxDepth();
        String baseUrl = crawlMedia.getLandingUrl();
        Integer page = crawlMedia.getIndexPageCount();
        int depth = 0;
        String topicUrl = getTopicUrl(domain);
        List<String> urlFilters = filterRepository.getAllUrlFilter();
        String newsPortal = getNameOfNewsPortal(domain);
        logger.info(String.format("[DEBUG] %s | Parsing Index Page", newsPortal));
        kafkaService.consumeFromKafka(topicUrl);
        List<String> linkList = new ArrayList<>();
        OkHttpClient client = new OkHttpClient();
        List<String> urlMessagesToKafka = new ArrayList<>();
        String url = "https://graphql-v4.kumparan.com/query?deduplicate=1";

        for (int j = 1; j <= page; j++) {
            String jsonBody = "[{\"operationName\":\"FindStoryFeedByChannelSlug\",\"variables\":{\"channelSlug\":\"news\",\"cursor\":\""
                    + j
                    + "\",\"size\":10,\"cursorType\":\"PAGE\",\"userAliasID\":\"53273844-33bb-4fa0-b20a-eacd9a0892d6\"},\"query\":\"query FindStoryFeedByChannelSlug($channelSlug: String!, $userAliasID: ID, $size: Int!, $cursor: String!, $cursorType: CursorType!) {\\n  FindStoryFeedByChannelSlug(\\n    channelSlug: $channelSlug\\n    userAliasID: $userAliasID\\n    cursorType: $cursorType\\n    size: $size\\n    cursor: $cursor\\n  ) {\\n    edges {\\n      ...CompactStory\\n      __typename\\n    }\\n    cursorInfo {\\n      ...CursorInfo\\n      __typename\\n    }\\n    __typename\\n  }\\n}\\n\"}]";

            RequestBody requestBody = RequestBody.create(MediaType.parse("application/json"), jsonBody.getBytes());

            Request request = new Request.Builder()
                    .url(url)
                    .header("authority", "graphql-v4.kumparan.com")
                    .header("accept", "*/*")
                    .header("accept-language", "en-US,en;q=0.9")
                    .header("content-type", "text/plain")
                    .header("deduplicate", "1")
                    .header("env-client", "a1833e44e2c236f8b39903ef49b856d5ebf05efdd8ef4513e58db32dfdeabe7299d15d1e7976b314efd400aca5fafeb1")
                    .header("origin", "https://kumparan.com")
                    .header("referer", "https://kumparan.com/")
                    .post(requestBody)
                    .build();

            try (Response response = client.newCall(request).execute()) {
                String responseBody = response.body().string();
                logger.info("Kumparan response: " + responseBody);
                JSONArray json = new JSONArray(responseBody).getJSONObject(0).getJSONObject("data")
                        .getJSONObject("FindStoryFeedByChannelSlug").getJSONArray("edges");
                for (int i = 0; i < json.length(); i++) {
                    String link = "https://kumparan.com/kumparannews/" + json.getJSONObject(i).getString("slug");
                    linkList.add(link);
                }
            } catch (IOException e) {
                logger.error("[ERROR] Failed to get response from Kumparan.com", e);
                continue;
            }
        }

        for (String href : linkList) {
            JSONObject jsonUrl = createJsonKafkaUrl(href, baseUrl, domain, domain, depth, "", "");
            if (href.length() != 0 && depth <= maxDepth && isValidLink(href, urlFilters)) {
                urlMessagesToKafka.add(jsonUrl.toString());
            }
        }
        logger.info(String.format("[DEBUG] %s | Sending news URL data to Kafka", newsPortal));
        kafkaService.sendBulkToKafka(urlMessagesToKafka, topicUrl);
        logger.info(String.format("[DEBUG] %s | Successfully sent news URL data to Kafka", newsPortal));
    }

    private String getTopicUrl(String domain) {
        return "url-" + domain.replaceAll("\\.(com|id|co)$", "");
    }

    private String getNameOfNewsPortal(String domain) {
        return domain.substring(0, 1).toUpperCase() + domain.substring(1);
    }

    private Boolean isValidLink(String link, List<String> filters) {
        for (String filter : filters) {
            if (link.matches(".*" + filter + ".*")) {
                return false;
            }
        }
        return true;
    }

    private JSONObject createJsonKafkaUrl(String url, String landingUrl, String originalDomain, String domain, int depth,
                                          String urlSelect, String contentSelect) throws IOException {
        Integer maxDepth = crawlMediaRepository.getNewsPortalByDomain(domain).getMaxDepth();
        JSONObject json = new JSONObject();
        json.put("url", url);
        json.put("landing_url", landingUrl);
        json.put("original_domain", originalDomain);
        json.put("last_checked", ZonedDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
        json.put("last_checked_ts", System.currentTimeMillis());
        json.put("domain", domain);
        json.put("depth", depth);
        json.put("max_depth", maxDepth);
        json.put("url_select", urlSelect);
        json.put("content_select", contentSelect);
        json.put("parse_auto", 1);
        return json;
    }
}