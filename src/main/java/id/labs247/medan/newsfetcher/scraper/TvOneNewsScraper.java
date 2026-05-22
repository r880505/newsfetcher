package id.labs247.medan.newsfetcher.scraper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

import okhttp3.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.json.JSONObject;
import id.labs247.medan.newsfetcher.repositories.CrawlMediaRepository;
import id.labs247.medan.newsfetcher.repositories.FilterRepository;
import id.labs247.medan.newsfetcher.models.CrawlMedia;


public class TvOneNewsScraper {

    private static final Logger logger = LogManager.getLogger(TvOneNewsScraper.class);
    private final String userAgent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";
    private final String userAgentClientHints = "\"Chromium\";v=\"130\", \"Google Chrome\";v=\"130\", \"Not?A_Brand\";v=\"99\"";

    private final CrawlMediaRepository crawlMediaRepository = new CrawlMediaRepository();
    private final FilterRepository filterRepository = new FilterRepository();
    private final KafkaService kafkaService = new KafkaService();
    private final OkHttpClient httpClient = new OkHttpClient.Builder()
            .connectTimeout(60, TimeUnit.SECONDS)
            .writeTimeout(60, TimeUnit.SECONDS)
            .readTimeout(60, TimeUnit.SECONDS)
            .build();

    public void parseIndexPage(String dateToParse, String topicUrl) throws IOException, Exception {
        String domain = "tvonenews.com";
        CrawlMedia crawlMedia = crawlMediaRepository.getNewsPortalByDomain(domain);
        String baseUrl = crawlMedia.getLandingUrl();
        Integer maxDepth = crawlMedia.getMaxDepth();
        String urlSelector = crawlMedia.getUrlSelect();
        String contentSelector = crawlMedia.getContentSelect();
        List<String> urlFilters = filterRepository.getAllUrlFilter();
        int depth = 0;
        String dateUrl1 = dateFormatter(dateToParse, "yyyy-MM-dd", "yyyy-MM-dd");
        String dateUrl2 = dateFormatter(dateToParse, "yyyy-MM-dd", "yyyy/MM/dd");
        String newsPortal = getNameOfNewsPortal(domain);
        logger.info(String.format("[DEBUG] %s | Parsing Index Page", newsPortal));
        kafkaService.consumeFromKafka(topicUrl);
        MediaType mediaType = MediaType.parse("application/x-www-form-urlencoded; charset=UTF-8");
        Integer page = crawlMedia.getIndexPageCount();

        List<String> urlMessagesToKafka = new ArrayList<>();

        for (int i = 1; i <= page; i++) {
            RequestBody body = RequestBody.create(mediaType,
                    "last_publish_date=" + dateUrl1 + "07%3A00%3A23&channel_name=berita&subchannel_name=all&page="
                            + i + "&type=art&record_count=12&_token=77618PRwzV9cJ5KCdhlWXiEYgjo9U9UqWv1qfjLM");

            Request request = new Request.Builder()
                    .url("https://www.tvonenews.com/request/load_indeks_article")
                    .method("POST", body)
                    .addHeader("authority", "www.tvonenews.com")
                    .addHeader("accept", "*/*")
                    .addHeader("accept-language", "en-US,en;q=0.9")
                    .addHeader("content-type", "application/x-www-form-urlencoded; charset=UTF-8")
                    .addHeader("cookie", "_cc_id=1c53434c430056ffe2701409b0d15c7f; ...")
                    .addHeader("origin", "https://www.tvonenews.com")
                    .addHeader("referer", baseUrl + dateUrl2 + "?type=art")
                    .addHeader("sec-ch-ua", userAgentClientHints)
                    .addHeader("sec-ch-ua-mobile", "?0")
                    .addHeader("sec-ch-ua-platform", "\"Linux\"")
                    .addHeader("sec-fetch-dest", "empty")
                    .addHeader("sec-fetch-mode", "cors")
                    .addHeader("sec-fetch-site", "same-origin")
                    .addHeader("user-agent", userAgent)
                    .addHeader("x-csrf-token", "77618PRwzV9cJ5KCdhlWXiEYgjo9U9UqWv1qfjLM")
                    .addHeader("x-requested-with", "XMLHttpRequest")
                    .build();

            Response response = httpClient.newCall(request).execute();
            try {
                Document document = Jsoup.parse(response.body().string());
                Elements elements = document.select(urlSelector);
                for (Element linkElement : elements) {
                    String href = linkElement.attr("href");
                    if (href.length() != 0 && depth <= maxDepth && isValidLink(href, urlFilters)) {
                        JSONObject jsonUrl = createJsonKafkaUrl(href, baseUrl + dateUrl1 + "?type=art", domain, domain,
                                depth, urlSelector, contentSelector);
                        urlMessagesToKafka.add(jsonUrl.toString());
                    }
                }
            } catch (IOException e) {
                logger.error(
                        String.format("[ERROR] %s | Failed to scrape Index Page | %s", newsPortal, e.getMessage()));
            }
        }
        logger.info(String.format("[DEBUG] %s | Sending news URL data to Kafka", newsPortal));
        kafkaService.sendBulkToKafka(urlMessagesToKafka, topicUrl);
        logger.info(String.format("[DEBUG] %s | Successfully sent news URL data to Kafka", newsPortal));
    }

    private String dateFormatter(String date, String currentFormat, String targetFormat) {
        DateTimeFormatter currentFormatter = DateTimeFormatter.ofPattern(currentFormat);
        LocalDate localDate = LocalDate.parse(date, currentFormatter);
        DateTimeFormatter targetFormatter = DateTimeFormatter.ofPattern(targetFormat);
        return localDate.format(targetFormatter);
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

    private JSONObject createJsonKafkaUrl(String url, String landingUrl, String originalDomain, String domain,
            int depth,
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