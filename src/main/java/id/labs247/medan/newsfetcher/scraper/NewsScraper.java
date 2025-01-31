package id.labs247.medan.newsfetcher.scraper;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.text.StringEscapeUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.common.SolrInputDocument;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.jayway.jsonpath.JsonPath;

import id.labs247.medan.newsfetcher.configs.ConfigurationLoader;
import id.labs247.medan.newsfetcher.models.CrawlExtraComment;
import id.labs247.medan.newsfetcher.models.CrawlMedia;
import id.labs247.medan.newsfetcher.models.NewsArticle;
import id.labs247.medan.newsfetcher.models.UrlFormat;
import id.labs247.medan.newsfetcher.repositories.CrawlMediaRepository;
import id.labs247.medan.newsfetcher.repositories.FilterRepository;
import id.labs247.medan.newsfetcher.repositories.FormatRepository;
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
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mmZ")    
    );

    private static int[] sleepDurations = {5000, 10000, 15000, 20000, 25000, 30000}; // Sleep durations in milliseconds

    /* ============================================================  Scraper ============================================================*/

    public void parseIndexPage(String domain, String dateToParse, String topicUrl, int page) throws Exception, IOException {

        // Get news portal name without domain .com, .co.id etc
        String newsPortal = getNameOfNewsPortal(domain);

        try {
            // Get metadata from database
            CrawlMedia crawlMedia = crawlMediaRepository.getNewsPortalByDomain(domain);
            String articleSelector = crawlMedia.getContentSelect();
            String linkSelector = crawlMedia.getUrlSelect();
            Integer maxDepth = crawlMedia.getMaxDepth();
            Long mediaId = crawlMedia.getMediaId();

            // Format date to standard format 'yyyy-MM-dd'
            String datePattern = formatRepository.getDateFormatById(crawlMedia.getDateFormatId());
            String date = dateFormatter(dateToParse, "yyyy-MM-dd", datePattern);

            // Instantiate and assign depth, depth indicate deep level of news
            int depth = 0;

            // Format url to standard
            UrlFormat urlFormat = formatRepository.getUrlFormatByMediaId(mediaId);
            String pageFormat = urlFormat.getFormat();
            int pageMultiplier = urlFormat.getMultiplier();
            int pageSubstractor = urlFormat.getSubstractor();

            // Get filter url
            List<String> urlFilters = filterRepository.getAllUrlFilter();

            // Consume kafka before produce
            logger.info(String.format("[DEBUG] %s | Parsing Index Page", newsPortal));
            kafkaService.consumeFromKafka(topicUrl);

            // Create array list to collect url
            List<String> urlMessagesToKafka = new ArrayList<>();

            for (int i = 1; i <= page; i++) {

                // Construct url standard format
                String url = generateUrl(pageFormat, date, i, pageMultiplier, pageSubstractor);
                try {

                    // Get news url from index page
                    List<String> articleLinks = getUrl(url, linkSelector);

                    for (String link : articleLinks) {

                        // Handle relative url with adding 'https://'
                        if(link.startsWith("/")) {
                            link = "https://" + domain + link;
                        }

                        // Create json from url
                        JSONObject jsonUrl = createJsonKafkaUrl(link, url, domain, domain, depth, linkSelector, articleSelector);

                        // Check url, if true add to array list
                        if (depth <= maxDepth && isValidLink(link, urlFilters) && !link.contains("#") && link.contains(domain)) {
                            urlMessagesToKafka.add(jsonUrl.toString());
                        }
                    }
                } catch (IOException e) {
                    continue;
                }
            }

            // Send url to kafka as string
            logger.info(String.format("[DEBUG] %s | Sending news URL data to Kafka", newsPortal));
            kafkaService.sendBulkToKafka(urlMessagesToKafka, topicUrl);
            logger.info(String.format("[DEBUG] %s | Successfully sent news URL data to Kafka", newsPortal));


        } catch (Exception e) {
            // Handle exception with send log
            logger.error(String.format("[ERROR] %s | Failed to scrape Index Page | %s", newsPortal, e.getMessage()), e);
        }
    }

    public void parseNews(String domain, boolean isRelatedNews, String topicUrl, String topicNews, String topicRelatedNewsUrl) throws IOException, Exception {
        
        // Get crawler metadata from database by domain
        CrawlMedia crawlMedia = crawlMediaRepository.getNewsPortalByDomain(domain);

        // Get selector for content, baca juga, and image
        List<String> selectorContentList = Arrays.asList(crawlMedia.getContentSelect().split(","));
        String selectorBacaJuga = crawlMedia.getBacajugaSelect();
        String selectorImage = crawlMedia.getImageSelect();
        List<String> imageSelectors = Arrays.asList(selectorImage);

        // Get page param for pagination, ex: ?page=all etc
        String pageParam = crawlMedia.getPageParam();

        // Get filter content 
        List<String> filterContent = filterRepository.getAllContentFilter();

        // Get filter url
        List<String> urlFilters = filterRepository.getAllUrlFilter();
        
        // Get news portal name without domain
        String newsPortal = getNameOfNewsPortal(domain);

        logger.info(String.format("[DEBUG] %s | Parsing %s", newsPortal, isRelatedNews ? "Related News" : "News"));

        // Consume from kafka before produce
        kafkaService.consumeFromKafka(topicNews);

        // Instantiation of topic to consume and/or produce
        String topic;
        
        // Create array list to collect data that consumed from kafka
        List<String> jsonFromKafkaList = new ArrayList<>();

        // Get list of JSON Url from Kafka (condition to scrape news or related news) and assign topic
        if(isRelatedNews) {
            jsonFromKafkaList = kafkaService.parsingKafkaResult(kafkaService.consumeFromKafka(topicRelatedNewsUrl));
            topic = topicRelatedNewsUrl;
        } else {
            kafkaService.consumeFromKafka(topicRelatedNewsUrl);
            jsonFromKafkaList = kafkaService.parsingKafkaResult(kafkaService.consumeFromKafka(topicUrl));
            topic = topicUrl;
        }

        logger.info(String.format("[DEBUG] Sum of URL received from Kafka %s: %d", topic, jsonFromKafkaList.size()));

        // Assign depth and max depth
        int depth = 0;
        int maxDepth = 0;

        Random random = new Random(); // Create a Random object for randomizing sleep durations

        // Instantiation of array list to collect news contents and baca juga urls
        List<String> contentMessagesToKafka = new ArrayList<>();
        List<String> bacaJugaMessagesToKafka = new ArrayList<>();
    
        // Iterate list of url to get content
        for (int i = 0; i < jsonFromKafkaList.size(); i++) {
            try {

                // Introduce random sleep after processing each URL
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

                // Create JSON from data that received from kafka
                JSONObject jsonToParse = new JSONObject(jsonFromKafkaList.get(i));
                
                // Parse url from JSON
                String url = jsonToParse.getString("url");

                // Assign depth and max depth from data that received from kafka
                depth = jsonToParse.getInt("depth");
                maxDepth = jsonToParse.getInt("max_depth");
    
                if (depth <= maxDepth) {
                    // Load url without pagination
                    Document document = getDocument(url);

                    // Load url with pagination
                    Document documentWithPagination = getDocument(url+pageParam);

                    // Add depth's value
                    depth += 1;

                    // Get content of news
                    // Iterate over selectorContentList until content is found
                    String content = "";
                    for (String selector : selectorContentList) {
                        content = getNews(documentWithPagination, parseFilterSelector(filterContent, selector));
                        if (!content.isEmpty()) {
                            break; // Stop if content is found
                        }
                    }
                    
                    // Remove all HTML's special characters
                    content = unescapeHTMLSpecialCharacter(content);

                    // Remove domain/domain's name and city before dash
                    content = removeBeforeDash(content);

                    // Send log if content is empty
                    if (content.isEmpty()) {
                        logger.warn(String.format("[WARNING] No content found for URL: %s using selectors: %s", url, selectorContentList));
                        continue;
                    }

                    // Parse author
                    List<String> author = parseAuthor(document);

                    // Parse image
                    String image = parseImage(document, imageSelectors);
                    if(image.startsWith("/")) {
                        image = "https://" + domain + image;
                    }

                    // Parse keywords
                    String[] keywords = parseKeywords(document);

                    // Parse title
                    String title = parseTitle(document);

                    // Parse date
                    String datePublished = parseDatePublished(document);

                    // Parse comments
                    JSONArray comments = new JSONArray();
                    if (crawlMedia.getExtraStatus()==1 && crawlMedia.getExtra().equals("c")) {
                        CrawlExtraComment crawlExtraComment = crawlMediaRepository.getCrawlExtraCommentByMediaId(crawlMedia.getMediaId());
                        String selectorArticleId = crawlExtraComment.getArticleIdSelect();
                        String articleId = parseArticleId(documentWithPagination, selectorArticleId);
                        String commentApi = crawlExtraComment.getCommentApi();
                        String requestMethod = crawlExtraComment.getRequestMethod();

                        String requestParam = crawlExtraComment.getRequestParam();
                        requestParam = completeRequestBodyOrParam(requestMethod, requestParam);

                        String requestBody = crawlExtraComment.getRequestBody();
                        requestBody = completeRequestBodyOrParam(requestBody, articleId);

                        String cookie = crawlExtraComment.getCookie();
                        String selectorComment = crawlExtraComment.getSelectorComment();

                        comments = parseComment(domain, url, commentApi, requestMethod, requestBody, requestParam, cookie, selectorComment);
                    }

                    // Create JSON Content and add to array list
                    JSONObject jsonNews = createJsonKafkaContent(url, domain, content, image, datePublished, title, author, keywords, comments);
                    if (!content.isEmpty()) {
                        contentMessagesToKafka.add(jsonNews.toString());
                    }

                    // Get baca juga urls
                    List<String> bacajugaLinks = getUrl(document, selectorBacaJuga);

                    // Iterate to collect baca juga urls
                    for (String bacajugaLink : bacajugaLinks) {

                        // Handle relative url with adding 'https://'
                        if(bacajugaLink.startsWith("/")) {
                            bacajugaLink = "https://" + domain + bacajugaLink;
                        }

                        // Create JSON for baca juga url and check, if true add to array list
                        JSONObject jsonUrl = createJsonKafkaUrl(bacajugaLink, url, domain, domain, depth, crawlMedia.getUrlSelect(), crawlMedia.getContentSelect());
                        if (depth <= maxDepth && isValidLink(bacajugaLink, urlFilters) && bacajugaLink.contains(domain) && !bacajugaLink.contains("#")) {
                            bacaJugaMessagesToKafka.add(jsonUrl.toString());
                        }
                    }

                }
            } catch (IOException | JSONException e) {
                // Handle exception with send log
                String url = new JSONObject(jsonFromKafkaList.get(i)).getString("url");
                logger.error(String.format("[ERROR] %s | Failed to scrape news | %s | %s", newsPortal, e.getMessage(), url), e);
                continue;
            }
        }

        // Send/produce news contents and baca juga urls to kafka
        logger.info(String.format("[DEBUG] %s | Sending news article data to Kafka", newsPortal));
        kafkaService.sendBulkToKafka(contentMessagesToKafka, topicNews);
        logger.info(String.format("[DEBUG] %s | Successfully sent news article data to Kafka", newsPortal));
        logger.info(String.format("[DEBUG] %s | Sending related news URL data to Kafka", newsPortal));
        kafkaService.sendBulkToKafka(bacaJugaMessagesToKafka, topicRelatedNewsUrl);
        logger.info(String.format("[DEBUG] %s | Successfully sent related news URL data to Kafka", newsPortal));
        
        // Handle for parsing related news
        if (isRelatedNews) {
            if (depth <= maxDepth && jsonFromKafkaList.size()!=0)
                parseNews(domain, isRelatedNews, topicUrl, topicNews, topicRelatedNewsUrl);
        }
    }

    public void parseTvonenewsIndexPage(String dateToParse, String topicUrl) throws IOException, Exception {

        String domain = "tvonenews.com";

        // Get data from database by domain
        CrawlMedia crawlMedia = crawlMediaRepository.getNewsPortalByDomain(domain);
        String baseUrl = crawlMedia.getLandingUrl();
        Integer maxDepth = crawlMedia.getMaxDepth();
        String urlSelector = crawlMedia.getUrlSelect();
        String contentSelector = crawlMedia.getContentSelect();

        // Get url filters
        List<String> urlFilters = filterRepository.getAllUrlFilter();

        // Instantiate and assign depth, depth indicate deep level of news
        int depth = 0;

        // Format date
        String dateUrl1 = dateFormatter(dateToParse, "yyyy-MM-dd", "yyyy-MM-dd");
        String dateUrl2 = dateFormatter(dateToParse, "yyyy-MM-dd", "yyyy/MM/dd");

        // Instantiate okhttp client
        OkHttpClient client = new OkHttpClient();

        // Get news portal name without domain
        String newsPortal = getNameOfNewsPortal(domain);
        logger.info(String.format("[DEBUG] %s | Parsing Index Page", newsPortal));

        // Consume kafka before produce
        kafkaService.consumeFromKafka(topicUrl);

        // Parsing url
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
                    .addHeader("cookie",
                            "_cc_id=1c53434c430056ffe2701409b0d15c7f; cto_bundle=eljo7V9RbEdjNDdFMzVwUkR5aE1oWExjaGJFWE55aXI5UzZpREVHY1pjSzBqYiUyRjdRRVlwRUxMY0kyVmZmUVVLT0diT1d4NlBlU1BiNEE3UHlQTTY3UXJqNGtPOWxmVFQ1ZFVjeHZ1eGYlMkZod0JudTVJbjA2Qnk3NmZ0YUpCaElpWUV1QU50OFR2ZERqUkNoZUlETG1XTm0wJTJCemdXZiUyQjBRJTJCbmxyRGZHaWpybHVRTEN1SUxDc0tMMkNRNUxoUW5iemJFSm9Fa2VnUG5aNnRwMzVuVWhSRXAyRDM0amolMkZoZmsyTHVzUjl4UVBQcFhHZERwYUV2WlA5OEwyOFFvNmVySjJLT24xZmNhMVNub0hSWlc4eW9mUHNmMmpaWUh1eUxZZkh6TUwlMkZzRnlVV2tGdk9odG93amR4T1ZtNURTZ044Y3pEcGlO; _ss_pp_id=d15475d9121217e21d71695016827599; __utmz=262966473.1706517353.7.6.utmcsr=google|utmccn=(organic)|utmcmd=organic|utmctr=(not%20provided); _gid=GA1.2.109274476.1706690624; panoramaId_expiry=1707295428233; panoramaId=099fc4901cb08971b3f07f8a5da3185ca02c233abff817e02d3c254cacbef90f; panoramaIdType=panoDevice; _td=2b93d62c-a4fb-486e-8b88-0f873db49129; _gat=1; __utma=262966473.1083217.1706077335.1706690625.1706750313.9; __utmc=262966473; __utmt_UA-158515037-1=1; _clck=12im9dg%7C2%7Cfiw%7C0%7C1484; 651dfdf3-f6cf-4dce-b88d-f9b4e8d85861=2c614bb1ef5d20dedfe09fdc059c6425; _ga_1F8KC7SHMP=GS1.1.1706750312.9.1.1706750341.31.0.0; _ga=GA1.1.1083217.1706077335; __utmb=262966473.5.10.1706750313; _ga_SKS0GZ01Z9=GS1.2.1706750312.9.1.1706750341.0.0.0; XSRF-TOKEN=eyJpdiI6IkIxUXJDWkZCS0h2SGhDbDA2VTNcL3d3PT0iLCJ2YWx1ZSI6IkJtbm90ZDF1RzkzTnFjYTdNWmVOemxYTXJ3N0pQMFdNRzlISUNVXC9YK1hnUGMwVjRZMHRIWjhIRVVtdkZOWjFTIiwibWFjIjoiZGIxMWQ4ZmM1ZDBlYWU5ODVmYTRjM2ExOWNhMTc2ZTM3NGNhN2QyZDM1YWI1NjRlNzhhMjAwOTMyMjg1MTM5NiJ9; tvonenews_session=eyJpdiI6IkhpQVlNcEtJUUtodHFhcWlaVlhiQkE9PSIsInZhbHVlIjoieUNKdFZEMGhSMUFRZE9QYysweXBhd0ZwdjEzd00xcEdTdHlONk9nUTN6anhyYkNZMlBDQjFHVzdLYWRHdW9YbiIsIm1hYyI6ImNjNThiN2I3NzMwZDBiMzFiZDg1NWM4Njg0MWE2MmU1ODFjODg2ODJmMzdlYmM4ZWYyMzQyOTc3YmU5NWQyM2MifQ%3D%3D; _clsk=z3kyd1%7C1706750342602%7C5%7C1%7Cx.clarity.ms%2Fcollect; FCNEC=%5B%5B%22AKsRol_vmBM14TOCSPFiILrI9jhaV03mAFL1HCGBP-kUDx6mZMQhASFaGaTmEQNDpcF5uu1vlapZWjBvSlIvcncO2CbDGQbvipXfk-k7rSn3dgyFaET_3OlE0vyorgWMb9MBnRrjhJ8pOXNICGapL_OMVfH_cU6sYQ%3D%3D%22%5D%5D")
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

            Response response = client.newCall(request).execute();
            try {
                Document document = Jsoup.parse(response.body().string());
                Elements elements = document.select(urlSelector);
                for (Element linkElement : elements) {
                    String href = linkElement.attr("href");
                    if (href.length() != 0 && depth <= maxDepth && isValidLink(href, urlFilters)) {
                        JSONObject jsonUrl = createJsonKafkaUrl(href, baseUrl + dateUrl1 + "?type=art", domain, domain, depth, urlSelector, contentSelector);

                        // Add url into array list
                        urlMessagesToKafka.add(jsonUrl.toString());
                    }
                }
            } catch (IOException e) {
                logger.error(String.format("[ERROR] %s | Failed to scrape Index Page | %s", newsPortal, e.getMessage()));
            }

        }

        // Send/produce url to kafka
        logger.info(String.format("[DEBUG] %s | Sending news URL data to Kafka", newsPortal));
        kafkaService.sendBulkToKafka(urlMessagesToKafka, topicUrl);
        logger.info(String.format("[DEBUG] %s | Successfully sent news URL data to Kafka", newsPortal));

    }

    public void parseKumparanIndexPage() throws IOException, Exception {
        try {
            String domain = "kumparan.com";

            // Get metadata from database by domain
            CrawlMedia crawlMedia = crawlMediaRepository.getNewsPortalByDomain(domain);
            Integer maxDepth = crawlMedia.getMaxDepth();
            String baseUrl = crawlMedia.getLandingUrl();
            Integer page = crawlMedia.getIndexPageCount();

            // Instantiate and assign depth, depth indicate deep level of news
            int depth = 0;

            // Get topic by domain
            String topicUrl = getTopicUrl(domain);

            // Get url filters
            List<String> urlFilters = filterRepository.getAllUrlFilter();

            // Get news portal name without domain .com, .co.id etc
            String newsPortal = getNameOfNewsPortal(domain);
            logger.info(String.format("[DEBUG] %s | Parsing Index Page", newsPortal));

            // Consume kafka before produce
            kafkaService.consumeFromKafka(topicUrl);

            // Instantiate array list to collect urls
            List<String> linkList = new ArrayList<>();

            // Instantiate okhttp client
            OkHttpClient client = new OkHttpClient();

            // instantiate array list to collect urls from kafka
            List<String> urlMessagesToKafka = new ArrayList<>();

            // Parsing urls
            String url = "https://graphql-v4.kumparan.com/query?deduplicate=1";
            for (int j = 1; j <= page; j++) {
                String jsonBody = "[{\"operationName\":\"FindStoryFeedByChannelSlug\",\"variables\":{\"channelSlug\":\"news\",\"cursor\":\""
                        + j
                        + "\",\"size\":10,\"cursorType\":\"PAGE\",\"userAliasID\":\"53273844-33bb-4fa0-b20a-eacd9a0892d6\"},\"query\":\"query FindStoryFeedByChannelSlug($channelSlug: String!, $userAliasID: ID, $size: Int!, $cursor: String!, $cursorType: CursorType!) {\\n  FindStoryFeedByChannelSlug(\\n    channelSlug: $channelSlug\\n    userAliasID: $userAliasID\\n    cursorType: $cursorType\\n    size: $size\\n    cursor: $cursor\\n  ) {\\n    edges {\\n      ...CompactStory\\n      __typename\\n    }\\n    cursorInfo {\\n      ...CursorInfo\\n      __typename\\n    }\\n    __typename\\n  }\\n}\\n\\nfragment CompactStory on Story {\\n  __typename\\n  id\\n  authorID\\n  title\\n  customTitle\\n  createdAt\\n  leadText\\n  publishedAt\\n  isAgeRestrictedContent\\n  slug\\n  isStickyStory\\n  isShowOnWeb\\n  isShowOnApp\\n  isDisableComment\\n  isDisableLike\\n  isDisableShare\\n  isSnackable\\n  metaDescription\\n  readTimeInMinutes\\n  storyAddOns {\\n    ...StoryAddOn\\n    __typename\\n  }\\n  author {\\n    ...SimpleUser\\n    __typename\\n  }\\n  publisher {\\n    ...SimplePublisher\\n    __typename\\n  }\\n  leadMedia {\\n    ...Media\\n    __typename\\n  }\\n  headline {\\n    ...Headline\\n    __typename\\n  }\\n  statistic {\\n    ...StoryStatistic\\n    __typename\\n  }\\n  readEligibility\\n  productInfo {\\n    ...Product\\n    __typename\\n  }\\n  isPrivate\\n  collection {\\n    ...CompactCollection\\n    __typename\\n  }\\n  internalTags\\n  deletedAt\\n  deletionInfo {\\n    __typename\\n    deleterType\\n    deletedBy {\\n      __typename\\n      id\\n      name\\n    }\\n  }\\n}\\n\\nfragment StoryAddOn on StoryAddOn {\\n  object {\\n    __typename\\n    ... on Polling {\\n      ...Polling\\n      __typename\\n    }\\n    ... on Gallery {\\n      ...Gallery\\n      __typename\\n    }\\n    ... on Form {\\n      ...Form\\n      __typename\\n    }\\n    ... on Recipe {\\n      ...Recipe\\n      __typename\\n    }\\n    ... on LiveBlog {\\n      ...LiveBlogAddOnDetail\\n      __typename\\n    }\\n  }\\n  addOnType\\n  __typename\\n}\\n\\nfragment Polling on Polling {\\n  __typename\\n  id\\n  name\\n  description\\n  mediaID\\n  startsAt\\n  endsAt\\n  questions {\\n    ...Question\\n    __typename\\n  }\\n}\\n\\nfragment Question on Question {\\n  id\\n  pollingID\\n  detail\\n  position\\n  choices {\\n    ...Choice\\n    __typename\\n  }\\n  __typename\\n}\\n\\nfragment Choice on Choice {\\n  id\\n  questionID\\n  detail\\n  mediaID\\n  position\\n  stats\\n  __typename\\n}\\n\\nfragment Gallery on Gallery {\\n  name\\n  description\\n  __typename\\n  id\\n  createdAt\\n  updatedAt\\n  galleryMedias {\\n    ...GalleryMedia\\n    __typename\\n  }\\n}\\n\\nfragment GalleryMedia on GalleryMedia {\\n  mediaID\\n  description\\n  caption\\n  media {\\n    ...Media\\n    __typename\\n  }\\n  __typename\\n}\\n\\nfragment Media on Media {\\n  id\\n  title\\n  description\\n  publicID\\n  externalURL\\n  awsS3Key\\n  height\\n  width\\n  locationName\\n  locationLat\\n  locationLon\\n  mediaType\\n  mediaSourceID\\n  photographer\\n  eventDate\\n  internalTags\\n  __typename\\n}\\n\\nfragment Form on Form {\\n  __typename\\n  id\\n  title\\n  description\\n  generateStatus\\n  lastGenerated\\n  createdBy {\\n    ...SimpleUser\\n    __typename\\n  }\\n  createdAt\\n  updatedAt\\n  endDate\\n  pages {\\n    ...FormPage\\n    __typename\\n  }\\n  respondent\\n  coverMedia {\\n    ...Media\\n    __typename\\n  }\\n  accentColor\\n  backgroundColor\\n  completedResponseTitle\\n  completedResponse\\n  formResponseConfirmationMail {\\n    ...FormResponseConfirmationMail\\n    __typename\\n  }\\n}\\n\\nfragment FormPage on FormPage {\\n  page\\n  questions {\\n    ...FormQuestion\\n    __typename\\n  }\\n  __typename\\n}\\n\\nfragment FormQuestion on FormQuestion {\\n  id\\n  formId\\n  type\\n  description\\n  title\\n  isRequired\\n  createdAt\\n  updatedAt\\n  addOns {\\n    ...QuestionAddOns\\n    __typename\\n  }\\n  __typename\\n}\\n\\nfragment QuestionAddOns on QuestionAddOns {\\n  colcount\\n  choicesOrder\\n  hasOther\\n  otherText\\n  optionsCaption\\n  hasNone\\n  hasSelectAll\\n  valueTrue\\n  valueFalse\\n  labelTrue\\n  labelFalse\\n  rows\\n  placeholder\\n  choices\\n  fileType\\n  __typename\\n}\\n\\nfragment SimpleUser on User {\\n  __typename\\n  id\\n  name\\n  username\\n  aboutMe\\n  isVerified\\n  profilePictureMedia {\\n    ...Media\\n    __typename\\n  }\\n  coverPictureMedia {\\n    ...Media\\n    __typename\\n  }\\n  status\\n  metaKeyword\\n  metaTitle\\n  metaDescription\\n  excludedFromSitemapAt\\n}\\n\\nfragment FormResponseConfirmationMail on FormResponseConfirmationMail {\\n  title\\n  header\\n  body\\n  hyperlinkURL\\n  hyperlinkTitle\\n  __typename\\n}\\n\\nfragment Recipe on Recipe {\\n  id\\n  name\\n  description\\n  mediaID\\n  media {\\n    ...Media\\n    __typename\\n  }\\n  portionSize\\n  ingredients\\n  instructions {\\n    step\\n    instruction\\n    __typename\\n  }\\n  cookingTime\\n  calories\\n  createdAt\\n  createdBy {\\n    ...SimpleUser\\n    __typename\\n  }\\n  updatedAt\\n  updatedBy {\\n    ...SimpleUser\\n    __typename\\n  }\\n  __typename\\n}\\n\\nfragment LiveBlogAddOnDetail on LiveBlog {\\n  id\\n  startsAt\\n  endsAt\\n  updatedAt\\n  __typename\\n}\\n\\nfragment Headline on Headline {\\n  storyID\\n  desktopMedia {\\n    ...Media\\n    __typename\\n  }\\n  mobileMedia {\\n    ...Media\\n    __typename\\n  }\\n  startTime\\n  endTime\\n  __typename\\n}\\n\\nfragment StoryStatistic on StoryStatistic {\\n  storyID\\n  commentCount\\n  likeCount\\n  __typename\\n}\\n\\nfragment SimplePublisher on Publisher {\\n  __typename\\n  id\\n  name\\n  slug\\n  description\\n  isVerified\\n  isPremium\\n  avatarMedia {\\n    ...Media\\n    __typename\\n  }\\n  instagramURL\\n  twitterURL\\n  facebookURL\\n  website\\n  isCorporateSubscriber\\n  organisation {\\n    ...Organisation\\n    __typename\\n  }\\n}\\n\\nfragment Organisation on Organisation {\\n  id\\n  name\\n  phone1\\n  address\\n  email\\n  companyName\\n  editorialInChief\\n  editorialCompositions {\\n    ...EditorialComposition\\n    __typename\\n  }\\n  organisationType\\n  __typename\\n}\\n\\nfragment EditorialComposition on EditorialComposition {\\n  id\\n  organisationID\\n  position\\n  names\\n  order\\n  __typename\\n}\\n\\nfragment Product on Product {\\n  id\\n  sku\\n  objectID\\n  objectType\\n  object {\\n    ... on Story {\\n      __typename\\n      id\\n      slug\\n      title\\n      leadText\\n      author {\\n        id\\n        username\\n        __typename\\n      }\\n      publisher {\\n        id\\n        slug\\n        __typename\\n      }\\n      leadMedia {\\n        id\\n        externalURL\\n        __typename\\n      }\\n    }\\n    ... on Collection {\\n      __typename\\n      id\\n      slug\\n      title\\n      description\\n      coverMedia {\\n        id\\n        externalURL\\n        __typename\\n      }\\n    }\\n    ... on SubscriptionPackage {\\n      ...SimpleSubscriptionPackage\\n      __typename\\n    }\\n    __typename\\n  }\\n  normalPrice {\\n    ...Money\\n    __typename\\n  }\\n  price {\\n    ...Money\\n    __typename\\n  }\\n  taxCategory {\\n    ...TaxCategory\\n    __typename\\n  }\\n  __typename\\n}\\n\\nfragment TaxCategory on TaxCategory {\\n  id\\n  name\\n  rateInPercentage\\n  isInclusive\\n  __typename\\n}\\n\\nfragment Money on Money {\\n  currencyCode\\n  units\\n  cents\\n  __typename\\n}\\n\\nfragment SimpleSubscriptionPackage on SubscriptionPackage {\\n  __typename\\n  id\\n  name\\n  subscriptionDescription: description\\n  isActive\\n  isRecurring\\n  period\\n  periodType\\n  gracePeriodInSeconds\\n  platform\\n}\\n\\nfragment CompactCollection on Collection {\\n  __typename\\n  id\\n  title\\n  isPinned\\n  slug\\n  createdAt\\n  readEligibility\\n  updatedAt\\n  lastStoryAddedAt\\n  description\\n  category\\n  coverMedia {\\n    ...Media\\n    __typename\\n  }\\n  readEligibility\\n  productInfo {\\n    ...Product\\n    __typename\\n  }\\n  storiesCount\\n  videoDescriptionLink\\n  premiumInfo {\\n    setPremiumAt\\n    __typename\\n  }\\n  kumparanPlusInfo {\\n    createdAt\\n    __typename\\n  }\\n  highlightedMenus {\\n    url\\n    title\\n    __typename\\n  }\\n  topics {\\n    ...Topic\\n    __typename\\n  }\\n  firstStoryAddedAt\\n  lastStoryAddedAt\\n}\\n\\nfragment Topic on Topic {\\n  __typename\\n  id\\n  name\\n  slug\\n  description\\n  isPremium\\n  coverMedia {\\n    ...Media\\n    __typename\\n  }\\n  metaName\\n  metaKeywordsV2\\n  metaDescription\\n  createdAt\\n  updatedAt\\n}\\n\\nfragment CursorInfo on CursorInfo {\\n  size\\n  count\\n  countPage\\n  hasMore\\n  cursor\\n  cursorType\\n  nextCursor\\n  __typename\\n}\\n\"}]";

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
                        .header("sec-ch-ua", userAgentClientHints)
                        .header("sec-ch-ua-mobile", "?0")
                        .header("sec-ch-ua-platform", "\"Linux\"")
                        .header("sec-fetch-dest", "empty")
                        .header("sec-fetch-mode", "cors")
                        .header("sec-fetch-site", "same-site")
                        .header("user-agent", userAgent)
                        .post(requestBody)
                        .build();

                try (Response response = client.newCall(request).execute()) {
                    JSONArray json = new JSONArray(response.body().string()).getJSONObject(0).getJSONObject("data")
                            .getJSONObject("FindStoryFeedByChannelSlug").getJSONArray("edges");
                    for (int i = 0; i < json.length(); i++) {
                        String link = "https://kumparan.com/kumparannews/" + json.getJSONObject(i).getString("slug");
                        linkList.add(link);
                    }
                } catch (IOException | JSONException e) {
                    logger.error("[ERROR] Failed to get response from Kumparan.com", e);
                    continue;
                }

            }

            for (String href : linkList) {
                JSONObject jsonUrl = createJsonKafkaUrl(href, baseUrl, domain, domain, depth, "", "");
                if (href.length() != 0 && depth <= maxDepth && isValidLink(href, urlFilters)) {

                    // Add url to array list
                    urlMessagesToKafka.add(jsonUrl.toString());
                }
            }

            // Send/produce urls to kafka
            logger.info(String.format("[DEBUG] %s | Sending news URL data to Kafka", newsPortal));
            kafkaService.sendBulkToKafka(urlMessagesToKafka, topicUrl);
            logger.info(String.format("[DEBUG] %s | Successfully sent news URL data to Kafka", newsPortal));

        } catch (IOException e) {
            String newsPortal = "Kumparan.com";
            logger.error(String.format("[ERROR] %s | Failed to scrape Index Page | %s", newsPortal, e.getMessage()), e);
        }
    }

    public void executeParseIndexPage(String domain, String dateToParse, String topicUrl, int page) throws IOException, Exception {
        switch (domain) {
            case "kumparan.com":
                parseKumparanIndexPage();
                break;
            case "tvonenews.com":
                parseTvonenewsIndexPage(dateToParse, topicUrl);
                break;
            default:
                parseIndexPage(domain, dateToParse, topicUrl, page);
                break;
        }
    }

    public void executeParseNews(String domain, String topicUrl, String topicNews, String topicRelatedNewsUrl) throws IOException, Exception {
        parseNews(domain, false, topicUrl, topicNews, topicRelatedNewsUrl);
    }

    public void executeParseRelatedNews(String domain, String topicUrl, String topicNews, String topicRelatedNewsUrl) throws IOException, Exception {
        parseNews(domain, true, topicUrl, topicNews, topicRelatedNewsUrl);
    }

    /* ======================================================= UTILITIES ======================================================- */

    // Date formatter for scrapping Index Page
    public String dateFormatter(LocalDateTime localDateTime, String format) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);
        return localDateTime.format(formatter);
    }

    // Format string date to desired format
    public String dateFormatter(String date, String currentFormat, String targetFormat) {
        // Parse string date to LocalDate using the current format
        DateTimeFormatter currentFormatter = DateTimeFormatter.ofPattern(currentFormat);
        LocalDate localDate = LocalDate.parse(date, currentFormatter);

        // Format LocalDate to the target format
        DateTimeFormatter targetFormatter = DateTimeFormatter.ofPattern(targetFormat);
        return localDate.format(targetFormatter);
    }

    // Scrape news by url
    public String getNews(String url, String selector) throws IOException {
        Elements paragraphs = getElements(url, selector);
    
        StringBuilder content = new StringBuilder();
        for(Element paragraph : paragraphs) {
            if(paragraph.text().toLowerCase().length() != 0) {
                content.append(paragraph.text().trim() + " ");
            }
        }
    
        // Adding space after dot if following by character (without space)
        String result = content.toString().trim();
    
        return result;
    }

    // Scrape news by document
    public String getNews(Document document, String selector) throws IOException {
        Elements paragraphs = getElements(document, selector);
    
        StringBuilder content = new StringBuilder();
        for(Element paragraph : paragraphs) {
            if(paragraph.text().toLowerCase().length() != 0) {
                content.append(paragraph.text().trim() + " ");
            }
        }
    
        // Adding space after dot if following by character (without space)
        String result = content.toString().trim();
    
        return result;
    }

    // Get URL from index page or "baca juga" by url
    public List<String> getUrl(String url, String selector) {
        List<String> result = new ArrayList<>();
        try {
            Elements elements = getElements(url, selector);

            for (Element element : elements) {
                String newsUrl = element.attr("href");
                if (newsUrl.length() != 0) {
                    result.add(newsUrl);
                }
            }
        } catch (IOException e) {
            logger.error("[ERROR] " + e.getMessage());
        }

        return result;
    }

    // Get URL from index page or "baca juga" by document
    public List<String> getUrl(Document document, String selector) {
        List<String> result = new ArrayList<>();
        try {
            Elements elements = getElements(document, selector);

            for (Element element : elements) {
                String newsUrl = element.attr("href");
                if (newsUrl.length() != 0) {
                    result.add(newsUrl);
                }
            }
        } catch (IOException e) {
            logger.error("[ERROR] " + e.getMessage());
        }

        return result;
    }

    // Get document with error handling
    public Document getDocument(String url) throws IOException {
        Connection.Response response = Jsoup.connect(url)
                                            .userAgent(userAgent)
                                            .referrer("https://www.google.com")
                                            .execute();

        // Check HTTP status code
        int statusCode = response.statusCode();
        if (statusCode != 200) {
            logger.error("[ERROR] | HTTP error: " + statusCode + " - " + response.statusMessage());
        }

        return response.parse();
    }

    // Get elements by Url and selector
    public Elements getElements(String url, String selector) throws IOException {
        if (selector != null && !selector.isEmpty()) {
            return getDocument(url).select(selector);
        } else {
            // Handle the case where cssQuery is empty
            logger.error("[ERROR] CSS query must not be empty");
            return new Elements(); // Return empty elements to avoid breaking execution
        }
    }

    // Get elements by document and selector
    public Elements getElements(Document document, String selector) throws IOException {
        if (selector != null && !selector.isEmpty()) {
            return document.select(selector);
        } else {
            // Handle the case where cssQuery is empty
            logger.error("[ERROR] CSS query must not be empty.");
            return new Elements(); // Return empty elements to avoid breaking execution
        }
    }

    // Regex domain (for Kafka topic)
    private String regexDomain(String domain) {
        String regex = "\\.(com|id|co)$|\\.[a-zA-Z]{2,}\\.id$|\\.co\\.[a-zA-Z]{2,}$";
        return domain.replaceAll(regex, "");
    }

    // Get Kafka topic for Url, Content and Baca juga
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
        return "news-" + regexDomain(domain)  + "-backdate";
    }

    public String getTopicBacaJugaBackdate(String domain) {
        return "bacajuga-" + regexDomain(domain)  + "-backdate";
    }

    // Get name of news portal
    private String getNameOfNewsPortal(String domain) {
        return domain.substring(0, 1).toUpperCase() + domain.substring(1);
    }

    // Generate url consist of base url, date and page (if exist)
    private String generateUrl(String baseUrl, String date, int page, int multiplier, int substractor) {
        page = (page-substractor)*multiplier;
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

    // Url checker for checking to avoid multiple same news before commit to solr
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

    // Insert news to Solr
    public void insertNewsToSolr(String topic) throws Exception {
        List<ConsumerRecord<String, String>> records = kafkaService.consumeFromKafka(topic);
        List<String> jsonContentFromKafka = kafkaService.parsingKafkaResult(records);

        logger.info("[DEBUG] Received news content from kafka content {}: {}", topic, jsonContentFromKafka.size());

        List<SolrInputDocument> solrInputDocuments = new ArrayList<>();

        for (String jsonContent : jsonContentFromKafka) {
            JSONObject jsonToParse = new JSONObject(jsonContent);
            String url = jsonToParse.getString("url");
            SolrInputDocument solrInputDocument = createSolrDocument(jsonToParse);
            // if (urlCheckerHBase(url) == 3) {
                solrInputDocuments.add(solrInputDocument);
            // } else {
            //     logger.info("[DEBUG] HBase | The URL is already in Solr | {}", url);
            // }
        }

        try {
            if (solrInputDocuments.size() != 0) {
                logger.info("[DEBUG] Solr | Bulk insert to Solr for topic " + topic);
                solrService.sendToSolr(solrInputDocuments);
                logger.info("[DEBUG] Solr | Successfully inserted to Solr for topic " + topic + " with " + solrInputDocuments.size() + " data");
            } else {
                logger.warn("[WARN] Solr | No data to insert to Solr for topic " + topic);
            }
        } catch (Exception e) {
            logger.error("[ERROR] Failed to process content to Solr", e);
        }

    }

    // Create Solr Document from JSON Object
    private SolrInputDocument createSolrDocument(JSONObject jsonToParse) {
        String content = jsonToParse.optString("content");
        String url = jsonToParse.optString("url");
        String image = jsonToParse.optString("image");
        String domain = jsonToParse.optString("domain");
        String date = jsonToParse.optString("datePublished");
        String title = jsonToParse.optString("title");
        String dateId = parseDatetimeToDateOnly(date);
        JSONArray commentsArray = jsonToParse.optJSONArray("comments");

        // Construct author as array
        JSONArray authorArray = jsonToParse.optJSONArray("author");
        String[] author = null;
        if (authorArray != null) {
            author = new String[authorArray.length()];
            for (int i = 0; i < authorArray.length(); i++) {
                author[i] = authorArray.optString(i);
            }
        }

        // Construct keywords as array
        JSONArray keywordsArray = jsonToParse.optJSONArray("keywords");
        String[] keywords = null;
        if (keywordsArray != null) {
            keywords = new String[keywordsArray.length()];
            for (int i = 0; i < keywordsArray.length(); i++) {
                keywords[i] = keywordsArray.optString(i);
            }
        }

        // Construct comments as JSON objects in array field
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
        document.addField("id", url);
        document.addField("content", content);
        document.addField("image", image);
        
        // date = convertToTimezone(date, "yyyy-MM-dd'T'HH:mm:ssXXX", "UTC");
        ZonedDateTime zdt = convertDatetimeToUTC(date);
        String formattedDate = zdt.format(DateTimeFormatter.ISO_INSTANT); // Konversi ke format ISO-8601
        document.addField("date", formattedDate);

        document.addField("title", title);
        document.addField("dateid", dateId);

        // Add author as array or list to Solr document
        if (author != null) {
            for (String authorName : author) {
                document.addField("author", authorName);
            }
        }

        // Add keywords as array
        if (keywords != null) {
            for (String keyword : keywords) {
                document.addField("keywords", keyword);
            }
        }

        // Add comments as JSON objects in array field
        if (!comments.isEmpty()) {
            document.addField("comments", comments);
        }

        document.addField("last_checked_ts", System.currentTimeMillis());

        // Convert date to UTC
        String lastChecked = convertDatetimeToUTC(LocalDateTime.now().toString() + "+07:00")
                            .format(DateTimeFormatter.ISO_INSTANT);
        document.addField("last_checked", lastChecked);

        return document;
    }

    
    // Create JSON for Kafka Content
    private JSONObject createJsonKafkaContent(String url, String domain, String content, String imageSource,
            String datePublished, String title, List<String> author, String[] keywords, JSONArray comments) {
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
        return json;
    }

    // Create JSON Url
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

    // Parse filter to be CSS query
    private String parseFilterSelector(List<String> filters, String baseSelector) {
        String selector = baseSelector;
        for (String string : filters) {
            selector +=  ":not(:contains(" + string + "))";
        }
        return selector;
    }
    
    // Get JSON metadata and remove HTML's tag like <script> etc
    private String getJSONMetadataAndRemoveHTMLTag(Document document) {
        String selectedScript = "";
        Elements elementsScript = document.select("script[type=application/ld+json]");
        for (Element elementScr : elementsScript) {
            String jsonContent = elementScr.html();
            if (jsonContent.contains("NewsArticle") || jsonContent.contains("Article") || 
                jsonContent.contains("\"@type\":\"NewsArticle\"") || jsonContent.contains("\"@type\":\"Article\"")) {
                selectedScript = jsonContent;
                break;
            }
        }
        return selectedScript.trim();
    }
        
    // Clean the JSON
    private String sanitizeJSON(String jsonString) {
        jsonString = jsonString.replaceAll("[\\n\\r]+", "");
        jsonString = jsonString.replaceAll("\",\\s+\\}", "\"}");
        jsonString = jsonString.replaceAll(",\\s+\\}", "}");
        return jsonString;
    }

    // Parse news article as list
    private List<NewsArticle> parseNewsArticles(String jsonString) {
        jsonString = sanitizeJSON(jsonString);
        Object json = new JSONTokener(jsonString).nextValue();
        List<NewsArticle> newsArticles = new ArrayList<>();
    
        if (json instanceof JSONObject) {
            JSONObject jsonObject = (JSONObject) json;
            if (jsonObject.has("@graph")) {

                // Parsing elemen @graph
                JSONArray graphArray = jsonObject.getJSONArray("@graph");
                for (int i = 0; i < graphArray.length(); i++) {
                    JSONObject graphObject = graphArray.getJSONObject(i);
                    NewsArticle newsArticle = parseJSONObject(graphObject);
                    if (newsArticle != null) {
                        newsArticles.add(newsArticle);
                    }
                }
            } else {
                NewsArticle newsArticle = parseJSONObject(jsonObject);
                if (newsArticle != null) {
                    newsArticles.add(newsArticle);
                }
            }
        } else if (json instanceof JSONArray) {
            newsArticles.addAll(parseJSONArray((JSONArray) json));
        }
    
        return newsArticles;
    }

    // Parse news artcle as single object
    private NewsArticle parseJSONObject(JSONObject jsonObject) {
        NewsArticle newsArticle = new NewsArticle();
        if (jsonObject.has("@type") && ("NewsArticle".equals(jsonObject.getString("@type"))  || "Article".equals(jsonObject.getString("@type"))) ) {
            newsArticle.setTitle(jsonObject.optString("headline"));

            // Check for "datePublished" first, if not found, use "publishedDate"
            String datePublished = jsonObject.has("datePublished") ? jsonObject.optString("datePublished") : jsonObject.optString("publishedDate");
            if(matchFormatter(datePublished)==false) {
                datePublished = jsonObject.optString("dateModified");
            }   
            newsArticle.setDatePublished(datePublished);

            // Check for author information in both "author" and "authors"
            if (jsonObject.has("author")) {
                newsArticle.setAuthor(parseAuthorFromJSON(jsonObject.get("author")));
            } else if (jsonObject.has("authors")) {
                newsArticle.setAuthor(parseAuthorFromJSON(jsonObject.get("authors")));
            }

            return newsArticle;
        }
        return null;
    }

    // Match datetime for handling patrse datetime
    private boolean matchFormatter(String dateString) {
        for (DateTimeFormatter formatter : formatters) {
            try {
                // Try parsing as OffsetDateTime
                OffsetDateTime.parse(dateString, formatter);
                return true;
            } catch (DateTimeParseException e) {
                // Try parsing as LocalDateTime if OffsetDateTime fails
                try {
                    LocalDateTime.parse(dateString, formatter);
                    return true;
                } catch (DateTimeParseException ignored) {
                    // Continue to next formatter if parsing fails
                }
            }
        }
        return false; // Return false if no formatter matched
    }

    private List<NewsArticle> parseJSONArray(JSONArray jsonArray) {
        List<NewsArticle> newsArticles = new ArrayList<>();
        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject jsonObject = jsonArray.optJSONObject(i);
            if (jsonObject != null) {
                NewsArticle newsArticle = parseJSONObject(jsonObject);
                if (newsArticle != null) {
                    newsArticles.add(newsArticle);
                }
            }
        }
        return newsArticles;
    }

    private List<NewsArticle> getNewsArticles(Document document) {
        String jsonPropOfNews = getJSONMetadataAndRemoveHTMLTag(document);
        return parseNewsArticles(jsonPropOfNews);
    }

    private String parseTitle(Document document) {
        List<NewsArticle> newsArticles = getNewsArticles(document);
        String title = "";
        for (NewsArticle newsArticle : newsArticles) {
            // Parse title and escape HTML's special characters
            title = unescapeHTMLSpecialCharacter(newsArticle.getTitle());
        }
        return title;
    }

    private List<String> parseAuthor(Document document) {
        List<NewsArticle> newsArticles = getNewsArticles(document);
        List<String> authors = new ArrayList<>();
        for (NewsArticle newsArticle : newsArticles) {
            List<String> articleAuthors = newsArticle.getAuthor();
            for (String author : articleAuthors) {
                authors.add(unescapeHTMLSpecialCharacter(author));
            }
        }
        return authors;
    }

    private String parseDatePublished(Document document) {
        List<NewsArticle> newsArticles = getNewsArticles(document);
        String datePublished = "";
        for (NewsArticle newsArticle : newsArticles) { 
            datePublished = standarizeDatetime(newsArticle.getDatePublished(), "Asia/Jakarta");
        }
        return datePublished;
    }

    private List<String> parseAuthorFromJSON(Object authorObject) {
        List<String> authors = new ArrayList<>();
    
        if (authorObject instanceof JSONObject) {
            JSONObject author = (JSONObject) authorObject;
    
            // Check for single author name
            if (author.has("name")) {
                authors.add(author.optString("name", ""));
            }
            
            // Check for comma-separated names in "names"
            if (author.has("names")) {
                String names = author.optString("names", "");
                String[] namesArray = names.split(",\\s*");  // Split by comma and optional spaces
                for (String name : namesArray) {
                    authors.add(name.trim());
                }
            }
        } else if (authorObject instanceof JSONArray) {
            JSONArray authorArray = (JSONArray) authorObject;
            for (int i = 0; i < authorArray.length(); i++) {
                JSONObject author = authorArray.optJSONObject(i);
                if (author != null && author.has("name")) {
                    authors.add(author.optString("name", ""));
                }
            }
        } else if (authorObject instanceof String) {
            authors.add((String) authorObject);
        }
    
        return authors;
    }

    private String parseImage(Document document, String selector) throws IOException {
        Element element = getElements(document, selector).first();
        if (element != null) {
            String[] attributes = {"data-src", "src", "content"};
            for (String attribute : attributes) {
                String attrValue = element.attr(attribute);
                if (!attrValue.isEmpty()) {
                    return attrValue;
                }
            }
        }
        return "";
    }

    private String parseImage(Document document, List<String> selectors) throws IOException {
        for (String selector : selectors) {
            Element element = getElements(document, selector).first();
            if (element != null) {
                String[] attributes = {"data-src", "src", "content"};
                for (String attribute : attributes) {
                    String attrValue = element.attr(attribute);
                    if (!attrValue.isEmpty()) {
                        return attrValue; // Return if any image found
                    }
                }
            }
        }
        return ""; // Return if not found image
    }

    private String unescapeHTMLSpecialCharacter(String textToUnescape) {
        return StringEscapeUtils.unescapeHtml4(textToUnescape);
    }

    private String[] parseKeywords(Document document) {
        // Select the meta tag with name="keywords"
        Element keywordsMetaTag = document.selectFirst("meta[name=keywords]");
    
        // Check if the meta tag and its content are not null or empty
        if (keywordsMetaTag != null) {
            String keywordsContent = keywordsMetaTag.attr("content");
    
            if (keywordsContent != null && !keywordsContent.trim().isEmpty()) {
                // Split by comma, trim each keyword, and filter out any empty keywords
                return keywordsContent.split(",\\s*");
            }
        }
    
        // Return an empty array if no valid keywords found
        return new String[]{};
    }


    private String parseDatetimeToStandardFormat(String dateString, String timezone) {
        for (DateTimeFormatter formatter : formatters) {
            try {
                // Try parsing as OffsetDateTime first
                OffsetDateTime odt = OffsetDateTime.parse(dateString, formatter);
                return odt.atZoneSameInstant(ZoneId.of(timezone)).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
            } catch (DateTimeParseException e) {
                // If parsing as OffsetDateTime fails, try as LocalDateTime
                try {
                    LocalDateTime ldt = LocalDateTime.parse(dateString, formatter);
                    return ldt.atZone(ZoneId.of(timezone)).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
                } catch (DateTimeParseException ignored) {
                    // Skip to try next formatter
                }
            }
        }
        // Return original dateString if no formatter matched
        return dateString;
    }

    private String parseDatetimeToDateOnly(String dateString) {
        for (DateTimeFormatter formatter : formatters) {
            try {
                OffsetDateTime odt = OffsetDateTime.parse(dateString, formatter);
                return odt.toLocalDate().toString();
            } catch (DateTimeParseException e) {
                // Skip to try next format
            }
            try {
                LocalDateTime ldt = LocalDateTime.parse(dateString, formatter);
                return ldt.toLocalDate().toString();
            } catch (DateTimeParseException e) {
                // Skip to try next format
            }
        }
        return dateString;
    }

    private String cleanAndParseDatetime(String dateString) {
        // Replace 'WIB' with 'T' (if exist)
        if(dateString.contains("WIB")) {
            dateString = dateString.replace("WIB", "T");
            return dateString;
        } else {
            return dateString;
        }
    }

    private String standarizeDatetime(String dateString, String timezone) {
        dateString = cleanAndParseDatetime(dateString);
        dateString = parseDatetimeToStandardFormat(dateString, timezone);
        return dateString;
    }

    private JSONArray parseComment(String domain, String url, String commentApi, String requestMethod, String requestBody, String requestParam, String cookie, String selector) {

        // Initiate okhttp
        OkHttpClient client = new OkHttpClient();

        // Create request with null value
        Request request = null;

        // Complete request to comment API with different case between 'POST' and 'GET'
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
                RequestBody fbBody = RequestBody.create(MediaType.parse("application/x-www-form-urlencoded"), requestBody);
                String referer = "https://web.facebook.com/plugins/feedback.php?app_id=505071156342030&channel=https%3A%2F%2Fstaticxx.facebook.com%2Fx%2Fconnect%2Fxd_arbiter%2F%3Fversion%3D46%23cb%3Df2928167dcf3f63d4%26domain%3Dwww." + domain + "%26is_canvas%3Dfalse%26origin%3Dhttps%253A%252F%252Fwww." + domain + "%252Ff90c4f4b51b8817a7%26relation%3Dparent.parent&container_width=640&height=100&href=" + encodeUrl(url) + "&locale=en_GB&numposts=2&order_by=reverse_time&sdk=joey&version=v15.0&width";
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
                throw new IllegalArgumentException("Unsupported request method: " + requestMethod);
        }

        // Create array for result
        JSONArray jsonArrayComment = new JSONArray();


        // Send request and het the response
        if(request!=null) {

            // Send request
            try (Response response = client.newCall(request).execute()) {
                if (response.isSuccessful()) {

                    // Get response body
                    String responseBody = response.body().string();

                    if("FB".equals(requestMethod)) {
                        responseBody = responseBody.substring(responseBody.indexOf("{"));
                    }
            
                    // Load selector JSON from database
                    JSONObject jsonSelector = new JSONObject(selector);
            
                    // Load jsonPath and selector
                    String jsonPath = jsonSelector.getString("jsonPath");
                    JSONArray selectorArray = jsonSelector.getJSONArray("selector");
            
                    // Extract comment by path
                    List<Map<String, Object>> commentList = JsonPath.parse(responseBody).read(jsonPath);

                    // Parse comment for facebook plugin
                    if("FB".equals(requestMethod)) {
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
                                
                                // Get field type and path from JSONObject
                                String fieldType = fieldDetails.getString("type");
                                String fieldPath = fieldDetails.optString("path", null);
                                
                                // Parse value by type
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
                                            Long timestamp = parseTimestamp(timestampString);
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

                                if(fieldName.equals("comment") && value.toString().contains("nodes")) {
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
                
                                if (fieldName.equals("like") && value == null) value = 0;
                                if (fieldName.equals("dislike") && value == null) value = 0;

                                if(fieldName.equals("like") && value != null) {
                                    like = Integer.valueOf(value.toString());
                                } 

                                if(fieldName.equals("dislike") && value != null) {
                                    dislike = Integer.valueOf(value.toString())*(-1);
                                } 

                                like = like + dislike;

                                if(!fieldName.equals("dislike")) {
                                    if(fieldName.equals("like")) {
                                        jsonCommentResult.put("like", like);
                                    } else {
                                        jsonCommentResult.put(fieldName, value);
                                    }
                                }
            
                            }

                            if(jsonCommentResult.getString("comment").length()!=0)
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

    private String removeTimezone(String dateString) {
        // Regex pattern to remove timezone (matches `+hh:mm` or `-hh:mm` at the end of the string)
        String pattern = "(\\+|\\-)[0-9]{2}:[0-9]{2}$";
        
        // Replace the timezone part with an empty string
        return dateString.replaceAll(pattern, "");
    }

    private static String completeRequestBodyOrParam(String requestString, String articleId) {
        return requestString.replace("articleId", articleId);
    }

    private String parseArticleId(Document document, String selector) {
        try {
            // Parse articleId
            String articleId = "";
            if (selector.contains("meta")) {
                articleId = document.selectFirst(selector).attr("content");
            } else {
                articleId = document.selectFirst(selector).text(); 
            }
    
            return articleId;
    
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private Long parseTimestamp(String timestampString) {
        for (DateTimeFormatter formatter : formatters) {
            try {
                // Try parsing the timestamp with each formatter
                OffsetDateTime dateTime = OffsetDateTime.parse(timestampString, formatter);
                return dateTime.toInstant().toEpochMilli(); // Convert to milliseconds since epoch
            } catch (Exception e) {
                // If parsing fails, continue with the next formatter
                continue;
            }
        }
        return null; // Return null if no formatter works
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
        // Detect long dash (em dash) in text
        String regex = "^[^\\u2014]*?[\\-–—\\u2014]+\\s*"; // include all type of dashes

        // Check -, --, –, or — found in first 50 characters
        int dashIndex = text.indexOf("-");
        int doubleDashIndex = text.indexOf("--");
        int enDashIndex = text.indexOf("–");
        int emDashIndex = text.indexOf("—");

        // If it found, clean all before dash
        if ((dashIndex >= 0 && dashIndex <= 50) ||
            (doubleDashIndex >= 0 && doubleDashIndex <= 50) ||
            (enDashIndex >= 0 && enDashIndex <= 50) ||
            (emDashIndex >= 0 && emDashIndex <= 50)) {
            return text.replaceAll(regex, "").trim();
        } else {
            return text; // return if doesn't found
        }
    }

    private String convertToTimezone(String inputTime, String format, String targetZone) {
        // Parse inputted time with given format
        DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern(format);
        ZonedDateTime zonedDateTime = ZonedDateTime.parse(inputTime, inputFormatter);

        // Convert to target timezone
        ZonedDateTime targetTime = zonedDateTime.withZoneSameInstant(ZoneId.of(targetZone));

        // Format to ISO 8601
        return targetTime.format(DateTimeFormatter.ISO_INSTANT);
    }

    private ZonedDateTime convertDatetimeToUTC(String date) {

        // Parse string dengan offset
        OffsetDateTime odt = OffsetDateTime.parse(date, DateTimeFormatter.ISO_OFFSET_DATE_TIME);

        // Konversi ke UTC
        return odt.toInstant().atZone(ZoneOffset.UTC);

    }


    

}

