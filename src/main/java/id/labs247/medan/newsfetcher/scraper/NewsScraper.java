package id.labs247.medan.newsfetcher.scraper;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.common.SolrInputDocument;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.jayway.jsonpath.JsonPath;

import id.labs247.medan.newsfetcher.configs.ConfigurationLoader;
import id.labs247.medan.newsfetcher.models.CrawlExtraComment;
import id.labs247.medan.newsfetcher.models.CrawlMedia;
import id.labs247.medan.newsfetcher.models.UrlFormat;
import id.labs247.medan.newsfetcher.repositories.CrawlMediaRepository;
import id.labs247.medan.newsfetcher.repositories.FilterRepository;
import id.labs247.medan.newsfetcher.repositories.FormatRepository;
import id.labs247.medan.newsfetcher.utils.DateUtils;
import id.labs247.medan.newsfetcher.utils.JsoupHelper;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class NewsScraper {

    private final String userAgent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";

    private final String userAgentClientHints = "\"Chromium\";v=\"130\", \"Google Chrome\";v=\"130\", \"Not?A_Brand\";v=\"99\"";

    private static final Logger logger = LogManager.getLogger(NewsScraper.class);

    private SolrService solrService = new SolrService();

    private KafkaService kafkaService = new KafkaService();

    private CrawlMediaRepository crawlMediaRepository = new CrawlMediaRepository();

    private FilterRepository filterRepository = new FilterRepository();

    private FormatRepository formatRepository = new FormatRepository();

    private String urlCheckerApi = ConfigurationLoader.getUrlCheckerApi();

    private static final List<DateTimeFormatter> formatters = Arrays.asList(
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssXXX"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZ"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss'Z'"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mmZ"));

    private static int[] sleepDurations = { 5000, 10000, 15000, 20000, 25000, 30000 }; // Sleep durations in
                                                                                       // milliseconds

    /*
     * ============================================================ Scraper
     * ============================================================
     */

    public void executeParseIndexPage(String domain, String dateToParse, String topicUrl, int page)
            throws Exception, IOException {
        String newsPortal = getNameOfNewsPortal(domain);

        try {
            CrawlMedia crawlMedia = crawlMediaRepository.getNewsPortalByDomain(domain);
            String linkSelector = crawlMedia.getUrlSelect();
            String articleSelector = crawlMedia.getContentSelect();
            Integer maxDepth = crawlMedia.getMaxDepth();
            Long mediaId = crawlMedia.getMediaId();
            String datePattern = formatRepository.getDateFormatById(crawlMedia.getDateFormatId());
            String date = DateUtils.dateFormatter(dateToParse, "yyyy-MM-dd", datePattern);
            int depth = 0;
            UrlFormat urlFormat = formatRepository.getUrlFormatByMediaId(mediaId);
            String pageFormat = urlFormat.getFormat();
            logger.info(String.format("[DEBUG] %s | url_format: %s", newsPortal, pageFormat));
            int pageMultiplier = urlFormat.getMultiplier();
            int pageSubstractor = urlFormat.getSubstractor();
            List<String> urlFilters = filterRepository.getAllUrlFilter();
            logger.info(String.format("[DEBUG] %s | Parsing Index Page", newsPortal));
            kafkaService.consumeFromKafka(topicUrl);
            List<String> urlMessagesToKafka = new ArrayList<>();

            for (int i = 1; i <= page; i++) {
                String url = generateUrl(pageFormat, date, i, pageMultiplier, pageSubstractor);
                try {
                    List<String> articleLinks = JsoupHelper.getUrl(url, linkSelector);

                    for (String link : articleLinks) {
                        if (link.startsWith("/")) {
                            link = "https://" + domain + link;
                        }
                        JSONObject jsonUrl = createJsonKafkaUrl(link, url, domain, domain, depth, linkSelector,
                                articleSelector);
                        if (depth <= maxDepth && isValidLink(link, urlFilters) && !link.contains("#")
                                && link.contains(domain)) {
                            urlMessagesToKafka.add(jsonUrl.toString());
                        }
                    }
                } catch (IOException e) {
                    continue;
                }
            }
            logger.info(String.format("[DEBUG] %s | Sending news URL data to Kafka", newsPortal));
            kafkaService.sendBulkToKafka(urlMessagesToKafka, topicUrl);
            logger.info(String.format("[DEBUG] %s | Successfully sent news URL data to Kafka", newsPortal));

        } catch (Exception e) {
            logger.error(String.format("[ERROR] %s | Failed to scrape Index Page | %s", newsPortal, e.getMessage()), e);
        }
    }

    public void executeParseRelatedNews(String domain, String topicUrl, String topicNews, String topicRelatedNewsUrl)
            throws Exception {
        parseNews(domain, true, topicUrl, topicNews, topicRelatedNewsUrl);
    }

    public void executeParseNews(String domain, String topicUrl, String topicNews, String topicRelatedNewsUrl)
            throws Exception {
        parseNews(domain, false, topicUrl, topicNews, topicRelatedNewsUrl);
    }

    public void parseNews(String domain, boolean isRelatedNews, String topicUrl, String topicNews,
            String topicRelatedNewsUrl) throws IOException, Exception {
        CrawlMedia crawlMedia = crawlMediaRepository.getNewsPortalByDomain(domain);
        if ((crawlMedia == null || crawlMedia.getMediaId() == null) && domain.contains(".")) {
            String[] parts = domain.split("\\.");
            if (parts.length > 2) {
                String baseDomain = parts[parts.length - 2] + "." + parts[parts.length - 1];
                CrawlMedia baseMedia = crawlMediaRepository.getNewsPortalByDomain(baseDomain);
                if (baseMedia != null && baseMedia.getMediaId() != null) {
                    crawlMedia = baseMedia;
                    logger.info(String.format("[INFO] Domain %s tidak ditemukan, menggunakan base domain %s", domain,
                            baseDomain));
                }
            }
        }
        List<String> selectorContentList = Arrays.asList(crawlMedia.getContentSelect().split(","));
        String selectorBacaJuga = crawlMedia.getBacajugaSelect();
        String selectorImage = crawlMedia.getImageSelect();
        List<String> imageSelectors = Arrays.asList(selectorImage);
        String pageParam = (crawlMedia.getPageParam() != null) ? crawlMedia.getPageParam() : "";
        List<String> filterContent = filterRepository.getAllContentFilter();
        List<String> urlFilters = filterRepository.getAllUrlFilter();
        String newsPortal = getNameOfNewsPortal(domain);

        logger.info(String.format("[DEBUG] %s | Parsing %s", newsPortal, isRelatedNews ? "Related News" : "News"));
        kafkaService.consumeFromKafka(topicNews);
        String topic;
        List<String> jsonFromKafkaList = new ArrayList<>();
        if (isRelatedNews) {
            jsonFromKafkaList = kafkaService.parsingKafkaResult(kafkaService.consumeFromKafka(topicRelatedNewsUrl));
            topic = topicRelatedNewsUrl;
        } else {
            kafkaService.consumeFromKafka(topicRelatedNewsUrl);
            jsonFromKafkaList = kafkaService.parsingKafkaResult(kafkaService.consumeFromKafka(topicUrl));
            topic = topicUrl;
        }

        logger.info(String.format("[DEBUG] Sum of URL received from Kafka %s: %d", topic, jsonFromKafkaList.size()));
        int depth = 0;
        int maxDepth = 0;

        Random random = new Random(); // Create a Random object for randomizing sleep durations
        List<String> contentMessagesToKafka = new ArrayList<>();
        List<String> bacaJugaMessagesToKafka = new ArrayList<>();
        for (int i = 0; i < jsonFromKafkaList.size(); i++) {
            try {
                if (random.nextBoolean()) { // Randomly decide if we should sleep
                    int randomIndex = random.nextInt(sleepDurations.length);
                    int sleepDuration = sleepDurations[randomIndex];

                    int sleepInSeconds = sleepDuration / 1000;
                    logger.info("[DEBUG] Sleeping for " + sleepInSeconds + " seconds");

                    try {
                        Thread.sleep(sleepDuration); // Sleep for the chosen duration
                    } catch (InterruptedException e) {
                        logger.error("[ERROR] Thread was interrupted: " + e.getMessage());
                    }
                }
                JSONObject jsonToParse = new JSONObject(jsonFromKafkaList.get(i));
                String url = jsonToParse.getString("url");

                if (url.contains("tirto.id")) {
                    logger.info("[INFO] Skipping tirto.id due to Cloudflare protection.");
                    continue;
                }

                depth = jsonToParse.getInt("depth");
                maxDepth = jsonToParse.getInt("max_depth");

                if (depth <= maxDepth) {
                    String param = (pageParam != null && !pageParam.isEmpty()) ? pageParam : "";
                    Document document = JsoupHelper.getDocument(url + param);

                    if (document == null) {
                        logger.error("[ERROR] Gagal mengambil halaman: " + url + param);
                        continue;
                    }

                    depth += 1;
                    StringBuilder contentBuilder = new StringBuilder();
                    String content = "";
                    for (String selector : selectorContentList) {
                        Elements elements = document.select(selector);
                        if (!elements.isEmpty()) {
                            for (Element element : elements) {
                                Element clone = element.clone();
                                clone.select(
                                        ".labelhead_1, .div-tag, .div-share, .div-comment, .br, .pagination, .page-links, .halaman, .card, .sidebar, .related, .ads, footer, header, #liste_terkait, .liste_terkait, .tags, .tag-list, .artikel-terkait, .related-article")
                                        .remove();

                                for (Element block : clone.select(
                                        "p, div, br, li, h1, h2, h3, h4, h5, h6, blockquote, ul, ol, table, tr, section, article, header, footer, strong, b, em, i, a, span")) {
                                    block.prepend("\\n");
                                    block.after("\\n");
                                }

                                String rawText = clone.text().replace("\\n", "\n").trim();
                                String[] lines = rawText.split("\n");

                                List<String> extraFilters = Arrays.asList(
                                        "Tampilkan Semua", "Editor", "Tags", "beritaTerkait", "SHARE:", "komentar",
                                        "Halaman:", "Editor:", "baca juga:", "simak juga:", "(**");

                                for (int j = 0; j < lines.length; j++) {
                                    String lineText = lines[j].trim();
                                    if (lineText.isEmpty() || lineText.equals(":") || lineText.length() < 3) {
                                        continue;
                                    }

                                    boolean isTrash = false;
                                    String matchedFilter = "";
                                    for (String filter : filterContent) {
                                        if (lineText.toLowerCase().contains(filter.toLowerCase())) {
                                            isTrash = true;
                                            matchedFilter = filter;
                                            break;
                                        }
                                    }
                                    if (!isTrash) {
                                        for (String filter : extraFilters) {
                                            if (lineText.toLowerCase().contains(filter.toLowerCase())) {
                                                isTrash = true;
                                                matchedFilter = filter;
                                                break;
                                            }
                                        }
                                    }

                                    if (isTrash) {
                                        boolean isBacaJuga = matchedFilter.toLowerCase().contains("juga");

                                        if (isBacaJuga) {
                                            int labelIdx = lineText.toLowerCase().indexOf(matchedFilter.toLowerCase());
                                            if (labelIdx > 50) {
                                                lineText = lineText.substring(0, labelIdx).trim();
                                                isTrash = false;
                                            } else {
                                                while (j + 1 < lines.length && lines[j + 1].trim().length() < 3)
                                                    j++;
                                                if (j + 1 < lines.length && lines[j + 1].trim().length() < 150) {
                                                    j++;
                                                }
                                                continue;
                                            }
                                        } else if (matchedFilter.equals("(**")
                                                || matchedFilter.equalsIgnoreCase("Editor")) {
                                            int labelIdx = lineText.toLowerCase().indexOf(matchedFilter.toLowerCase());
                                            if (labelIdx > 50) {
                                                lineText = lineText.substring(0, labelIdx).trim();
                                                isTrash = false;
                                            } else {
                                                while (j + 1 < lines.length && lines[j + 1].trim().length() < 3)
                                                    j++;
                                                if (j + 1 < lines.length) {
                                                    String nextLine = lines[j + 1].trim();
                                                    if (nextLine.length() < 80 && !nextLine.startsWith("\"")) {
                                                        j++;
                                                    }
                                                }
                                                continue;
                                            }
                                        } else {
                                            if (lineText.length() < 80) {
                                                while (j + 1 < lines.length && (lines[j + 1].trim().isEmpty()
                                                        || lines[j + 1].trim().length() < 3))
                                                    j++;
                                                if (j + 1 < lines.length && lines[j + 1].trim().length() < 120) {
                                                    j++;
                                                }
                                            }
                                            continue;
                                        }
                                    }

                                    String cleanLine = JsoupHelper.unescapeHTMLSpecialCharacter(lineText);
                                    cleanLine = filterAntaraContent(cleanLine);
                                    contentBuilder.append(cleanLine).append("\n\n");
                                }
                            }
                            content = contentBuilder.toString().trim();
                            if (!content.isEmpty()) {
                                content = removeBeforeDash(content);
                                break;
                            }
                        }
                    }
                    String cleanText = cleanTextExceptLetter(content);
                    if (content.isEmpty()) {
                        logger.warn(String.format("[WARNING] No content found for URL: %s using selectors: %s", url,
                                selectorContentList));
                        continue;
                    }
                    List<String> author = JsoupHelper.parseAuthor(document);
                    String image = JsoupHelper.parseImage(document, imageSelectors);
                    if (image.startsWith("/")) {
                        image = "https://" + domain + image;
                    }
                    String[] keywords = JsoupHelper.parseKeywords(document);
                    String title = JsoupHelper.parseTitle(document);
                    String datePublished = JsoupHelper.parseDatePublished(document);
                    JSONArray comments = new JSONArray();
                    if (crawlMedia.getExtraStatus() == 1 && crawlMedia.getExtra().equals("c")) {
                        CrawlExtraComment crawlExtraComment = crawlMediaRepository
                                .getCrawlExtraCommentByMediaId(crawlMedia.getMediaId());
                        String selectorArticleId = crawlExtraComment.getArticleIdSelect();
                        String articleId = JsoupHelper.parseArticleId(document, selectorArticleId);
                        String commentApi = crawlExtraComment.getCommentApi();
                        String requestMethod = crawlExtraComment.getRequestMethod();

                        String requestParam = crawlExtraComment.getRequestParam();
                        requestParam = completeRequestBodyOrParam(requestMethod, requestParam);

                        String requestBody = crawlExtraComment.getRequestBody();
                        requestBody = completeRequestBodyOrParam(requestBody, articleId);

                        String cookie = crawlExtraComment.getCookie();
                        String selectorComment = crawlExtraComment.getSelectorComment();

                        comments = parseComment(domain, url, commentApi, requestMethod, requestBody, requestParam,
                                cookie, selectorComment);
                    }
                    JSONObject jsonNews = createJsonKafkaContent(url, domain, content, image, datePublished, title,
                            author, keywords, comments, cleanText);
                    if (!content.isEmpty()) {
                        logger.info(String.format(
                                "[INFO] %s | Successfully scraped news article: %s | Saved to Kafka topic: %s",
                                newsPortal, url, topicNews));
                        contentMessagesToKafka.add(jsonNews.toString());
                    }
                    List<String> bacajugaLinks = JsoupHelper.getUrl(document, selectorBacaJuga);
                    for (String bacajugaLink : bacajugaLinks) {
                        if (bacajugaLink.startsWith("/")) {
                            bacajugaLink = "https://" + domain + bacajugaLink;
                        }
                        JSONObject jsonUrl = createJsonKafkaUrl(bacajugaLink, url, domain, domain, depth,
                                crawlMedia.getUrlSelect(), crawlMedia.getContentSelect());
                        if (depth <= maxDepth && isValidLink(bacajugaLink, urlFilters) && bacajugaLink.contains(domain)
                                && !bacajugaLink.contains("#")) {
                            bacaJugaMessagesToKafka.add(jsonUrl.toString());
                        }
                    }
                }
            } catch (IOException | JSONException e) {
                String url = new JSONObject(jsonFromKafkaList.get(i)).getString("url");
                logger.error(
                        String.format("[ERROR] %s | Failed to scrape news | %s | %s", newsPortal, e.getMessage(), url),
                        e);
                continue;
            }
        }
        logger.info(String.format("[DEBUG] %s | Sending news article data to Kafka", newsPortal));
        kafkaService.sendBulkToKafka(contentMessagesToKafka, topicNews);
        logger.info(String.format("[DEBUG] %s | Successfully sent news article data to Kafka", newsPortal));
        logger.info(String.format("[DEBUG] %s | Sending related news URL data to Kafka", newsPortal));
        kafkaService.sendBulkToKafka(bacaJugaMessagesToKafka, topicRelatedNewsUrl);
        logger.info(String.format("[DEBUG] %s | Successfully sent related news URL data to Kafka", newsPortal));
        if (isRelatedNews) {
            if (depth <= maxDepth && jsonFromKafkaList.size() != 0)
                parseNews(domain, isRelatedNews, topicUrl, topicNews, topicRelatedNewsUrl);
        }
    }

    /*
     * ======================================================= UTILITIES
     * ======================================================-
     */
    public String dateFormatter(LocalDateTime localDateTime, String format) {
        return DateUtils.dateFormatter(localDateTime, format);
    }

    public String dateFormatter(String date, String currentFormat, String targetFormat) {
        return DateUtils.dateFormatter(date, currentFormat, targetFormat);
    }

    private String regexDomain(String domain) {
        String regex = "\\.(com|id|co)$|\\.[a-zA-Z]{2,}\\.id$|\\.co\\.[a-zA-Z]{2,}$";
        return domain.replaceAll(regex, "");
    }

    public String getTopicUrl(String domain) {
        return "url-" + regexDomain(domain);
    }

    public String getTopicContent(String domain) {
        return "news-" + regexDomain(domain);
    }

    public String getTopicBacaJuga(String domain) {
        return "bacajuga-" + regexDomain(domain);
    }

    public String getTopicUrlBackdate(String domain) {
        return "url-" + regexDomain(domain) + "-backdate";
    }

    public String getTopicContentBackdate(String domain) {
        return "news-" + regexDomain(domain) + "-backdate";
    }

    public String getTopicBacaJugaBackdate(String domain) {
        return "bacajuga-" + regexDomain(domain) + "-backdate";
    }

    private String getNameOfNewsPortal(String domain) {
        return domain.substring(0, 1).toUpperCase() + domain.substring(1);
    }

    private String generateUrl(String baseUrl, String date, int page, int multiplier, int substractor) {
        page = (page - substractor) * multiplier;
        return baseUrl.replace("{date}", date).replace("{page}", String.valueOf(page));
    }

    private Boolean isValidLink(String link, List<String> filters) {
        for (String filter : filters) {
            if (link.matches(".*" + filter + ".*")) {
                return false;
            }
        }
        return true;
    }

    private int urlCheckerHBase(String url) throws IOException {
        String data = "url=" + URLEncoder.encode(url, "UTF-8");
        MediaType mediaType = MediaType.parse("application/x-www-form-urlencoded");

        OkHttpClient client = new OkHttpClient();

        RequestBody body = RequestBody.create(mediaType, data);
        Request request = new Request.Builder()
                .url(urlCheckerApi)
                .post(body)
                .build();

        try (Response response = client.newCall(request).execute()) {
            if (response.code() != 200) {
                logger.error("[ERROR] Unexpected HTTP status code: " + response.code() + " for URL: " + url);
                return -1;
            }

            String responseBody = response.body().string();
            JSONObject jsonResponse = new JSONObject(responseBody);
            return jsonResponse.getInt("response_code");
        } catch (IOException e) {
            logger.error("[ERROR] Failed to check URL with HBase: " + e.getMessage(), e);
            throw e;
        }
    }

    public void insertNewsToSolr(String topic) throws Exception {
        List<ConsumerRecord<String, String>> records = kafkaService.consumeFromKafka(topic);
        List<String> jsonContentFromKafka = kafkaService.parsingKafkaResult(records);

        logger.info("[DEBUG] Received news content from kafka content {}: {}", topic, jsonContentFromKafka.size());

        List<SolrInputDocument> solrInputDocuments = new ArrayList<>();

        for (String jsonContent : jsonContentFromKafka) {
            JSONObject jsonToParse = new JSONObject(jsonContent);
            String url = jsonToParse.getString("url");
            SolrInputDocument solrInputDocument = createSolrDocument(jsonToParse);
            if (urlCheckerHBase(url) == 3) {
                solrInputDocuments.add(solrInputDocument);
            } else {
                logger.info("[DEBUG] HBase | The URL is already in Solr | {}", url);
            }
        }

        try {
            if (solrInputDocuments.size() != 0) {
                logger.info("[DEBUG] Solr | Bulk insert to Solr for topic " + topic);
                solrService.sendToSolr(solrInputDocuments);
                logger.info("[DEBUG] Solr | Successfully inserted to Solr for topic " + topic + " with "
                        + solrInputDocuments.size() + " data");
            } else {
                logger.warn("[WARN] Solr | No data to insert to Solr for topic " + topic);
            }
        } catch (Exception e) {
            logger.error("[ERROR] Failed to process content to Solr", e);
        }

    }

    private SolrInputDocument createSolrDocument(JSONObject jsonToParse) {
        String content = jsonToParse.optString("content");
        String cleanText = jsonToParse.optString("clean_text");
        String url = jsonToParse.optString("url");
        String image = jsonToParse.optString("image");
        String domain = jsonToParse.optString("domain");
        String date = jsonToParse.optString("datePublished");
        String title = jsonToParse.optString("title");
        String dateId = DateUtils.parseDatetimeToDateOnly(date);
        String solrDocId = generateUuid();
        JSONArray commentsArray = jsonToParse.optJSONArray("comments");
        JSONArray authorArray = jsonToParse.optJSONArray("author");
        String[] author = null;
        if (authorArray != null) {
            author = new String[authorArray.length()];
            for (int i = 0; i < authorArray.length(); i++) {
                author[i] = authorArray.optString(i);
            }
        }
        JSONArray keywordsArray = jsonToParse.optJSONArray("keywords");
        String[] keywords = null;
        if (keywordsArray != null) {
            keywords = new String[keywordsArray.length()];
            for (int i = 0; i < keywordsArray.length(); i++) {
                keywords[i] = keywordsArray.optString(i);
            }
        }
        List<String> comments = new ArrayList<>();
        if (commentsArray != null) {
            for (int i = 0; i < commentsArray.length(); i++) {
                JSONObject commentObject = commentsArray.optJSONObject(i);
                if (commentObject != null) {
                    comments.add(commentObject.toString());
                }
            }
        }

        SolrInputDocument document = new SolrInputDocument();
        document.addField("domain", domain);
        document.addField("url", url);
        document.addField("id", solrDocId);
        document.addField("content", content);
        document.addField("image", image);
        document.addField("clean_text", cleanText);
        ZonedDateTime zdt = DateUtils.convertDatetimeToUTC(date);
        String formattedDate = zdt.format(DateTimeFormatter.ISO_INSTANT); // Konversi ke format ISO-8601
        document.addField("date", formattedDate);
        document.addField("title", title);
        document.addField("dateid", dateId);
        if (author != null) {
            for (String authorName : author) {
                document.addField("author__ms", authorName);
            }
        }
        if (keywords != null) {
            for (String keyword : keywords) {
                document.addField("keywords", keyword);
            }
        }
        if (!comments.isEmpty()) {
            document.addField("comments", comments);
        }

        document.addField("last_checked_ts", System.currentTimeMillis());
        String lastChecked = DateUtils.convertDatetimeToUTC(LocalDateTime.now().toString() + "+07:00")
                .format(DateTimeFormatter.ISO_INSTANT);
        document.addField("last_checked", lastChecked);

        String processDate = DateUtils.convertDatetimeToUTC(LocalDateTime.now().toString() + "+07:00")
                .format(DateTimeFormatter.ISO_INSTANT);
        document.addField("processDate", processDate);

        return document;
    }

    private JSONObject createJsonKafkaContent(String url, String domain, String content, String imageSource,
            String datePublished, String title, List<String> author, String[] keywords, JSONArray comments,
            String cleanText) {
        JSONObject json = new JSONObject();
        json.put("url", url);
        json.put("domain", domain);
        json.put("content", content);
        json.put("last_checked", LocalDateTime.now().toString());
        json.put("last_checked_ts", System.currentTimeMillis());
        json.put("image", imageSource);
        json.put("datePublished", datePublished);
        json.put("title", title);
        json.put("author", new JSONArray(author));
        json.put("keywords", new JSONArray(keywords));
        json.put("comments", comments);
        json.put("clean_text", cleanText);
        return json;
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

    private String parseFilterSelector(List<String> filters, String baseSelector) {
        String selector = baseSelector;
        for (String string : filters) {
            selector += ":not(:contains(" + string + "))";
        }
        return selector;
    }

    private JSONArray parseComment(String domain, String url, String commentApi, String requestMethod,
            String requestBody, String requestParam, String cookie, String selector) {
        OkHttpClient client = new OkHttpClient();
        Request request = null;
        switch (requestMethod) {
            case "POST":
                RequestBody postBody = RequestBody.create(MediaType.get("application/json"), requestBody);
                request = new Request.Builder()
                        .url(commentApi)
                        .post(postBody)
                        .addHeader("accept", "application/json")
                        .addHeader("accept-language", "en-US,en;q=0.9")
                        .addHeader("content-type", "application/json")
                        .addHeader("cookie", cookie)
                        .addHeader("origin", "https://" + domain)
                        .addHeader("priority", "1")
                        .addHeader("referer", "https://" + domain)
                        .addHeader("sec-ch-ua", userAgentClientHints)
                        .addHeader("sec-ch-ua-mobile", "?0")
                        .addHeader("sec-ch-ua-platform", "\"Linux\"")
                        .addHeader("sec-fetch-dest", "empty")
                        .addHeader("sec-fetch-mode", "cors")
                        .addHeader("sec-fetch-site", "same-site")
                        .addHeader("user-agent", userAgent)
                        .build();
                break;

            case "GET":
                request = new Request.Builder()
                        .url(commentApi + requestParam)
                        .get()
                        .build();
                break;

            case "FB":
                RequestBody fbBody = RequestBody.create(MediaType.parse("application/x-www-form-urlencoded"),
                        requestBody);
                String referer = "https://web.facebook.com/plugins/feedback.php?app_id=505071156342030&channel=https%3A%2F%2Fstaticxx.facebook.com%2Fx%2Fconnect%2Fxd_arbiter%2F%3Fversion%3D46%23cb%3Df2928167dcf3f63d4%26domain%3Dwww."
                        + domain + "%26is_canvas%3Dfalse%26origin%3Dhttps%253A%252F%252Fwww." + domain
                        + "%252Ff90c4f4b51b8817a7%26relation%3Dparent.parent&container_width=640&height=100&href="
                        + encodeUrl(url)
                        + "&locale=en_GB&numposts=2&order_by=reverse_time&sdk=joey&version=v15.0&width";
                request = new Request.Builder()
                        .url(commentApi)
                        .post(fbBody)
                        .header("accept", "*/*")
                        .header("accept-language", "en-US,en;q=0.9")
                        .header("content-type", "application/x-www-form-urlencoded")
                        .header("cookie", cookie)
                        .header("origin", "https://web.facebook.com")
                        .header("priority", "u=1, i")
                        .header("referer", referer)
                        .header("sec-ch-ua", userAgentClientHints)
                        .header("sec-ch-ua-mobile", "?0")
                        .header("sec-ch-ua-platform", "\"Linux\"")
                        .header("sec-fetch-dest", "empty")
                        .header("sec-fetch-mode", "cors")
                        .header("sec-fetch-site", "same-origin")
                        .header("user-agent", userAgent)
                        .build();
                break;

            default:
                logger.info("Unsupported request method: " + requestMethod);
        }
        JSONArray jsonArrayComment = new JSONArray();
        if (request != null) {
            try (Response response = client.newCall(request).execute()) {
                if (response.isSuccessful()) {
                    String responseBody = response.body().string();

                    if (responseBody == null || responseBody.trim().isEmpty()) {
                        logger.warn("[WARN] Empty response body when fetching comments for URL: " + url);
                        return jsonArrayComment;
                    }

                    if (!responseBody.trim().startsWith("{") && !responseBody.trim().startsWith("[")) {
                        logger.warn("[WARN] Response body is not a valid JSON object or array for URL: " + url);
                        return jsonArrayComment;
                    }

                    if ("FB".equals(requestMethod)) {
                        responseBody = responseBody.substring(responseBody.indexOf("{"));
                    }
                    JSONObject jsonSelector = new JSONObject(selector);
                    String jsonPath = jsonSelector.getString("jsonPath");
                    JSONArray selectorArray = jsonSelector.getJSONArray("selector");
                    List<Map<String, Object>> commentList = JsonPath.parse(responseBody).read(jsonPath);
                    if ("FB".equals(requestMethod)) {
                        List<Map<String, Object>> combinedData = new ArrayList<>();
                        for (Map<String, Object> comment : commentList) {
                            String authorID = (String) comment.get("authorID");
                            Map<String, Object> authorInfo = commentList.stream()
                                    .filter(author -> author.get("id").equals(authorID))
                                    .findFirst()
                                    .orElse(null);

                            if (authorInfo != null) {
                                Map<String, Object> combined = new HashMap<>();
                                combined.put("name", authorInfo.get("name"));
                                combined.put("comment", ((Map<String, Object>) comment.get("body")).get("text"));
                                combined.put("timestamp", ((Map<String, Object>) comment.get("timestamp")).get("time"));
                                combined.put("like", comment.get("likeCount"));
                                combinedData.add(combined);
                            }
                        }

                        jsonArrayComment = new JSONArray(combinedData);

                    } else {

                        for (Map<String, Object> obj : commentList) {
                            JSONObject jsonCommentResult = new JSONObject();

                            Integer like = 0;
                            Integer dislike = 0;

                            for (int i = 0; i < selectorArray.length(); i++) {
                                JSONObject field = selectorArray.getJSONObject(i);
                                String fieldName = field.keys().next();
                                JSONObject fieldDetails = field.getJSONObject(fieldName);
                                String fieldType = fieldDetails.getString("type");
                                String fieldPath = fieldDetails.optString("path", null);
                                Object value = null;
                                if (fieldPath != null) {
                                    switch (fieldType) {
                                        case "String":
                                            value = JsonPath.parse(obj).read(fieldPath, String.class);
                                            break;
                                        case "Integer":
                                            value = JsonPath.parse(obj).read(fieldPath, Integer.class);
                                            break;
                                        case "OffsetDateTime":
                                        case "StringDateTime":
                                            String timestampString = JsonPath.parse(obj).read(fieldPath, String.class);
                                            Long timestamp = DateUtils.parseTimestamp(timestampString);
                                            value = timestamp;
                                            break;
                                        case "StringTimestamp":
                                            String timestampStr = JsonPath.parse(obj).read(fieldPath, String.class);
                                            Double timestampDouble = Double.valueOf(timestampStr);
                                            Long ts = timestampDouble.longValue();
                                            value = ts;
                                            break;
                                        case "null":
                                            value = null;
                                            break;
                                    }
                                }

                                if (fieldName.equals("comment") && value.toString().contains("nodes")) {
                                    JSONObject commentJsonObject = new JSONObject(value.toString());

                                    String textComment = commentJsonObject
                                            .getJSONObject("document")
                                            .getJSONArray("nodes")
                                            .getJSONObject(0)
                                            .getJSONArray("nodes")
                                            .getJSONObject(0)
                                            .getJSONArray("leaves")
                                            .getJSONObject(0)
                                            .getString("text");
                                    value = textComment;
                                }

                                if (fieldName.equals("like") && value == null)
                                    value = 0;
                                if (fieldName.equals("dislike") && value == null)
                                    value = 0;

                                if (fieldName.equals("like") && value != null) {
                                    like = Integer.valueOf(value.toString());
                                }

                                if (fieldName.equals("dislike") && value != null) {
                                    dislike = Integer.valueOf(value.toString()) * (-1);
                                }

                                like = like + dislike;

                                if (!fieldName.equals("dislike")) {
                                    if (fieldName.equals("like")) {
                                        jsonCommentResult.put("like", like);
                                    } else {
                                        jsonCommentResult.put(fieldName, value);
                                    }
                                }

                            }

                            if (jsonCommentResult.getString("comment").length() != 0)
                                jsonArrayComment.put(jsonCommentResult);
                        }

                    }

                } else {
                    logger.error("[ERROR] Request failed: " + response.code());
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            logger.error("No request sent, please check each parameter, request body or header");
        }

        return jsonArrayComment;

    }

    private static String completeRequestBodyOrParam(String requestString, String articleId) {
        return requestString.replace("articleId", articleId);
    }

    private String encodeUrl(String url) {
        try {
            return URLEncoder.encode(url, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return null;
        }
    }

    private String removeBeforeDash(String text) {
        String regex = "^[^\\u2014]*?[\\-–—\\u2014]+\\s*"; // include all type of dashes
        int dashIndex = text.indexOf("-");
        int doubleDashIndex = text.indexOf("--");
        int enDashIndex = text.indexOf("–");
        int emDashIndex = text.indexOf("—");
        if ((dashIndex >= 0 && dashIndex <= 50) ||
                (doubleDashIndex >= 0 && doubleDashIndex <= 50) ||
                (enDashIndex >= 0 && enDashIndex <= 50) ||
                (emDashIndex >= 0 && emDashIndex <= 50)) {
            return text.replaceAll(regex, "").trim();
        } else {
            return text; // return if doesn't found
        }
    }

    private ZonedDateTime convertDatetimeToUTC(String date) {
        OffsetDateTime odt = OffsetDateTime.parse(date, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        return odt.toInstant().atZone(ZoneOffset.UTC);

    }

    private String filterAntaraContent(String content) {
        String regex = "\\s*(?:Penerjemah:|Pewarta:|Editor:).*?Copyright © ANTARA \\d{4}";
        return content.replaceAll(regex, "");
    }

    private String cleanTextExceptLetter(String text) {
        String regex = "[^a-zA-Z ]";
        return text.replaceAll(regex, "").replaceAll("\\s{2,}", " ").trim();
    }

    private String generateUuid() {
        UUID uuid = UUID.randomUUID();
        String uuidString = uuid.toString();
        return uuidString;
    }

}