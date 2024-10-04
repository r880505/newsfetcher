package id.labs247.medan.newsfetcher.scraper;

import java.io.IOException;
import java.net.URLEncoder;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.LocalDate;
import java.util.ArrayList;
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

import id.labs247.medan.newsfetcher.configs.ConfigurationLoader;
import id.labs247.medan.newsfetcher.models.CrawlMedia;
import id.labs247.medan.newsfetcher.models.NewsArticle;
import id.labs247.medan.newsfetcher.repositories.ContentFilterRepository;
import id.labs247.medan.newsfetcher.repositories.CrawlMediaRepository;
import id.labs247.medan.newsfetcher.repositories.DateFormatRepository;
import id.labs247.medan.newsfetcher.repositories.UrlFilterRepository;
import id.labs247.medan.newsfetcher.repositories.UrlFormatRepository;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class NewsScraper {

    private final String userAgent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";

    private static final Logger logger = LogManager.getLogger(NewsScraper.class);

    private SolrService solrService = new SolrService();

    private KafkaService kafkaService = new KafkaService();

    private CrawlMediaRepository crawlMediaRepository = new CrawlMediaRepository();

    private ContentFilterRepository contentFilterRepository = new ContentFilterRepository();

    private UrlFilterRepository urlFilterRepository = new UrlFilterRepository();

    private UrlFormatRepository urlFormatRepository = new UrlFormatRepository();

    private DateFormatRepository dateFormatRepository = new DateFormatRepository();

    private String urlCheckerApi = ConfigurationLoader.getUrlCheckerApi();

    /* ============================================================  Scraper ============================================================*/

    public void parseIndexPage(String domain, String dateToParse, String topicUrl, int page) throws Exception, IOException {

        String newsPortal = getNewsPortal(domain);

        try {
            // Get metadata from DB
            CrawlMedia crawlMedia = crawlMediaRepository.getByDomain(domain);
            String articleSelector = crawlMedia.getContentSelect();
            String linkSelector = crawlMedia.getUrlSelect();
            Integer maxDepth = crawlMedia.getMaxDepth();
            // String baseUrl = crawlMedia.getLandingUrl();
            Long mediaId = crawlMedia.getMediaOnlineSchedulerId();

            // Get date
            String datePattern = dateFormatRepository.getDateFormatById(crawlMedia.getDateFormatId());
            String date = dateFormatter(dateToParse, "yyyy-MM-dd", datePattern);

            int depth = 0;

            // Get url format
            String urlFormat = urlFormatRepository.getByMediaId(mediaId).getFormat();
            int pageMultiplier = urlFormatRepository.getByMediaId(mediaId).getMultiplier();
            int pageSubstractor = urlFormatRepository.getByMediaId(mediaId).getSubstractor();

            // Get filter url
            List<String> urlFilters = urlFilterRepository.getAllUrlFilter();

            logger.info(String.format("[DEBUG] %s | Parsing Index Page", newsPortal));
            kafkaService.subscribeFromKafka(topicUrl);

            for (int i = 1; i <= page; i++) {
                // Get url pattern
                String url = generateUrl(urlFormat, date, i, pageMultiplier, pageSubstractor);
                try {
                    // Get url from index page
                    List<String> articleLinks = getUrl(url, linkSelector);

                    // Iterate send to kafka
                    for (String link : articleLinks) {
                        if(link.startsWith("/")) {
                            link = "https://" + domain + link;
                        }
                        JSONObject jsonUrl = createJsonKafkaUrl(link, url, domain, domain, depth, linkSelector, articleSelector);
                        if (depth <= maxDepth && isValidLink(link, urlFilters) && !link.contains("#") && link.contains(domain)) {
                            logger.info(String.format("[DEBUG] %s | Sending news URL to Kafka | %s", newsPortal, link));
                            kafkaService.sendToKafka(jsonUrl.toString(), topicUrl);
                        }
                    }
                } catch (IOException e) {
                    continue;
                }
            }

        } catch (Exception e) {
            logger.error(String.format("[ERROR] %s | Failed to scrape Index Page | %s", newsPortal, e.getMessage()), e);
        }
    }

    public void parseNews(String domain, boolean isRelatedNews, String topicUrl, String topicNews, String topicRelatedNewsUrl) throws IOException, Exception {
        // Get crawler metadata from db by domain
        CrawlMedia crawlMedia = crawlMediaRepository.getByDomain(domain);

        // Get content, baca juga, image and author selector
        String selectorContent = crawlMedia.getContentSelect();
        String selectorBacaJuga = crawlMedia.getBacajugaSelect();
        String selectorImage = crawlMedia.getImageSelect();
        String selectorAuthor = crawlMedia.getAuthorSelect();
        String pageParam = crawlMedia.getPageParam();

        // Get filter content 
        List<String> filterContent = contentFilterRepository.getAllContentFilter();
        String filterNews = parseFilterSelector(filterContent, selectorContent);

        // Get filter url
        List<String> urlFilters = urlFilterRepository.getAllUrlFilter();
    
        String newsPortal = getNewsPortal(domain);
        logger.info(String.format("[DEBUG] %s | Parsing %s", newsPortal, isRelatedNews ? "Related News" : "News"));

        kafkaService.subscribeFromKafka(topicNews);

        String topic;
        
        List<String> jsonFromKafkaList = new ArrayList<>();
        // Get list of JSON Url from Kafka (condition to scrape news or related news)
        if(isRelatedNews) {
            jsonFromKafkaList = kafkaService.parsingKafka(kafkaService.subscribeFromKafka(topicRelatedNewsUrl));
            topic = topicRelatedNewsUrl;
        } else {
            kafkaService.subscribeFromKafka(topicRelatedNewsUrl);
            jsonFromKafkaList = kafkaService.parsingKafka(kafkaService.subscribeFromKafka(topicUrl));
            topic = topicUrl;
        }

        logger.info(String.format("[DEBUG] Sum of URL received from Kafka %s: %d", topic, jsonFromKafkaList.size()));

        int depth = 0;
        int maxDepth = 0;

        Random random = new Random(); // Create a Random object for randomizing sleep durations
        int[] sleepDurations = {5000, 10000, 15000, 20000, 25000, 30000}; // Sleep durations in milliseconds
    
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

                JSONObject jsonToParse = new JSONObject(jsonFromKafkaList.get(i));
    
                String url = jsonToParse.getString("url");
                depth = jsonToParse.getInt("depth");
                maxDepth = jsonToParse.getInt("max_depth");
    
                if (depth <= maxDepth) {
                    Document document = getDocument(url+pageParam);
                    depth += 1;

                    // Get content of news
                    String content = getNews(url+pageParam, filterNews);
                    content = unescapeHTMLSpecialCharacter(content);

                    String jsonPropOfNews = getJSONMetadataAndRemoveHTMLTag(document);
                    List<NewsArticle> newsArticles = parseNewsArticle(jsonPropOfNews);
    
                    for (NewsArticle newsArticle : newsArticles) {
    
                        // Clean author and convert to array
                        String author = parseAuthor(url, selectorAuthor);
                        author = cleanAuthor(author);
                        String[] authorArray = convertAuthorToArray(author);
    
                        String title = getTitle(newsArticle);
                        title = unescapeHTMLSpecialCharacter(title);
                        String image = parseImage(url, selectorImage);
    
                        // Clean published date
                        String publishedDate = getPublishedDate(newsArticle);
                        publishedDate = standarizeDatetime(publishedDate);
        
                        // Create JSON Content and send to Kafka
                        JSONObject jsonNews = createJsonKafkaContent(url, domain, content, image, publishedDate, title, authorArray);
                        if (content.length() != 0) {
                            logger.info(String.format("[DEBUG] %s | Sending news article to Kafka | %s", newsPortal, url));
                            kafkaService.sendToKafka(jsonNews.toString(), topicNews);
                        }
                    }

                    // Get baca juga Url and then send to Kafka
                    List<String> bacajugaLinks = getUrl(url, selectorBacaJuga);

                    // Iterate to send to kafka
                    for (String bacajugaLink : bacajugaLinks) {
                        if(bacajugaLink.startsWith("/")) {
                            bacajugaLink = "https://" + domain + bacajugaLink;
                        }
                        JSONObject jsonUrl = createJsonKafkaUrl(bacajugaLink, url, domain, domain, depth, crawlMedia.getUrlSelect(), crawlMedia.getContentSelect());
                        if (depth <= maxDepth && isValidLink(bacajugaLink, urlFilters) && bacajugaLink.contains(domain) && !bacajugaLink.contains("#")) {
                            logger.info(String.format("[DEBUG] %s | Sending related news URL to Kafka | %s", newsPortal, bacajugaLink));
                            kafkaService.sendToKafka(jsonUrl.toString(), topicRelatedNewsUrl);
                        }
                    }

                }
            } catch (IOException | JSONException e) {
                String url = new JSONObject(jsonFromKafkaList.get(i)).getString("url");
                logger.error(
                        String.format("[ERROR] %s | Failed to scrape news | %s | %s", newsPortal, e.getMessage(), url), e);
                continue;
            }
        }
    
        if (isRelatedNews) {
            if (depth <= maxDepth && jsonFromKafkaList.size()!=0)
                parseNews(domain, isRelatedNews, topicUrl, topicNews, topicRelatedNewsUrl);
        }
    }

    public void parseTvonenewsIndexPage(String dateToParse, String topicUrl) throws IOException, Exception {

        String domain = "tvonenews.com";
        CrawlMedia crawlMedia = crawlMediaRepository.getByDomain(domain);
        String baseUrl = crawlMedia.getLandingUrl();
        Integer maxDepth = crawlMedia.getMaxDepth();
        int depth = 0;

        String urlSelector = crawlMedia.getUrlSelect();
        String contentSelector = crawlMedia.getContentSelect();

        // Get filter url
        List<String> urlFilters = urlFilterRepository.getAllUrlFilter();

        String dateUrl1 = dateFormatter(dateToParse, "yyyy-MM-dd", "yyyy-MM-dd");
        String dateUrl2 = dateFormatter(dateToParse, "yyyy-MM-dd", "yyyy/MM/dd");

        OkHttpClient client = new OkHttpClient();

        String newsPortal = getNewsPortal(domain);
        logger.info(String.format("[DEBUG] %s | Parsing Index Page", newsPortal));

        kafkaService.subscribeFromKafka(topicUrl);

        MediaType mediaType = MediaType.parse("application/x-www-form-urlencoded; charset=UTF-8");
        Integer page = crawlMedia.getIndexPageCount();
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
                    .addHeader("sec-ch-ua",
                            "\"Not A(Brand\";v=\"99\", \"Google Chrome\";v=\"121\", \"Chromium\";v=\"121\"")
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
                        logger.info(String.format("[DEBUG] %s | Sending news URL to Kafka | %s", newsPortal, href));
                        kafkaService.sendToKafka(jsonUrl.toString(), topicUrl);
                    }
                }
            } catch (IOException e) {
                logger.error(String.format("[ERROR] %s | Failed to scrape Index Page | %s", newsPortal, e.getMessage()));
            }

        }

    }

    public void parseKumparanIndexPage() throws IOException, Exception {
        try {
            String domain = "kumparan.com";
            CrawlMedia crawlMedia = crawlMediaRepository.getByDomain(domain);
            Integer maxDepth = crawlMedia.getMaxDepth();
            int depth = 0;
            String topic = getTopicUrl(domain);
            String baseUrl = crawlMedia.getLandingUrl();
            Integer page = crawlMedia.getIndexPageCount();

            String newsPortal = getNewsPortal(domain);

            // Get filter url
            List<String> urlFilters = urlFilterRepository.getAllUrlFilter();

            logger.info(String.format("[DEBUG] %s | Parsing Index Page", newsPortal));
            kafkaService.subscribeFromKafka(topic);

            List<String> linkList = new ArrayList<>();

            OkHttpClient client = new OkHttpClient();

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
                        .header("env-client",
                                "a1833e44e2c236f8b39903ef49b856d5ebf05efdd8ef4513e58db32dfdeabe7299d15d1e7976b314efd400aca5fafeb1")
                        .header("origin", "https://kumparan.com")
                        .header("referer", "https://kumparan.com/")
                        .header("sec-ch-ua",
                                "\"Not A(Brand\";v=\"99\", \"Google Chrome\";v=\"121\", \"Chromium\";v=\"121\"")
                        .header("sec-ch-ua-mobile", "?0")
                        .header("sec-ch-ua-platform", "\"Linux\"")
                        .header("sec-fetch-dest", "empty")
                        .header("sec-fetch-mode", "cors")
                        .header("sec-fetch-site", "same-site")
                        .header("user-agent",
                                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
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
                    logger.info(String.format("[DEBUG] %s | Sending news URL to Kafka | %s", newsPortal, href));
                    kafkaService.sendToKafka(jsonUrl.toString(), topic);
                }
            }

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

    public void insertToSolr(String topic) throws Exception {
        contentToSolr(topic);
    }

    /* ======================================================= UTILITIES ======================================================- */

    // Date formatter for scrapping Index Page
    public String dateFormatter(LocalDateTime localDateTime, String format) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);
        return localDateTime.format(formatter);
    }

    // Method to format string date to desired format
    public String dateFormatter(String date, String currentFormat, String targetFormat) {
        // Parse string date to LocalDate using the current format
        DateTimeFormatter currentFormatter = DateTimeFormatter.ofPattern(currentFormat);
        LocalDate localDate = LocalDate.parse(date, currentFormatter);

        // Format LocalDate to the target format
        DateTimeFormatter targetFormatter = DateTimeFormatter.ofPattern(targetFormat);
        return localDate.format(targetFormatter);
    }

    // Scrape news
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

    public List<Map<String, Object>> getNewsFromIndex(String indexPageUrl, String indexPageSelector, String newsSelector) throws IOException {
        Elements elements = getElements(indexPageUrl, indexPageSelector);
        List<Map<String, Object>> result = new ArrayList<>();
    
        for(Element element : elements) {
            Map<String, Object> entry = new HashMap<>();
            String newsUrl = element.attr("href");
            if (newsUrl.length()!=0) {
                String news = getNews(newsUrl, newsSelector);
                if (news.length()!=0) {
                    entry.put("url", newsUrl);
                    entry.put("news", news);
                    result.add(entry);
                }
            }
        }
        return result;
    }

    // Get URL from index page or "baca juga"
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

    // Get elements
    public Elements getElements(String url, String selector) throws IOException {
        return getDocument(url).select(selector);
    }

    // Regex domain (for Kafka topic)
    private String regexDomain(String domain) {
        String regex = "\\.com$|\\.id$|\\.co$|\\.co.id$";
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
    private String getNewsPortal(String domain) {
        return domain.substring(0, 1).toUpperCase() + domain.substring(1);
    }

    // Generate url consist of base url, date and page (if exist)
    public String generateUrl(String baseUrl, String date, int page, int multiplier, int substractor) {
        page = (page-substractor)*multiplier;
        return baseUrl.replace("{date}", date).replace("{page}", String.valueOf(page));
    }

    private Boolean isValidLink(String link, List<String> filters) {
        for (String filter : filters) {
            if (link.contains(filter)) {
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

    public void contentToSolr(String topic) throws Exception {
        List<ConsumerRecord<String, String>> records = kafkaService.subscribeFromKafka(topic);
        List<String> jsonContentFromKafka = kafkaService.parsingKafka(records);

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

        for(SolrInputDocument document :solrInputDocuments) {
            try {
                logger.info("[DEBUG] Solr | Inserting to Solr: " + document.get("url"));
                solrService.sendToSolr(document);
            } catch (Exception e) {
                logger.error("[ERROR] Failed to process content for URL", e);
            }
        }

    }

    private SolrInputDocument createSolrDocument(JSONObject jsonToParse) {
        String content = jsonToParse.optString("content");
        String url = jsonToParse.optString("url");
        String image = jsonToParse.optString("image");
        String domain = jsonToParse.optString("domain");
        String date = jsonToParse.optString("publishedDate");
        String title = jsonToParse.optString("title");
        String dateId = date.substring(0, date.indexOf("T"));

        // Construct author as JSONArray
        JSONArray authorArray = jsonToParse.optJSONArray("author");
        String[] author = null;
        if (authorArray != null) {
            author = new String[authorArray.length()];
            for (int i = 0; i < authorArray.length(); i++) {
                author[i] = authorArray.optString(i);
            }
        }

        SolrInputDocument document = new SolrInputDocument();
        document.addField("domain", domain);
        document.addField("url", url);
        // document.addField("id", url);
        document.addField("content", content);
        document.addField("image", image);
        document.addField("date", date);
        document.addField("dateid", dateId);
        document.addField("title", title);

        // Add author as array or list to Solr document
        if (author != null) {
            for (String authorName : author) {
                document.addField("author", authorName);
            }
        }

        document.addField("last_checked_ts", System.currentTimeMillis());
        document.addField("last_checked", LocalDateTime.now().toString());

        return document;
    }

    public String removeTimezone(String dateString) {
        // Find the position of '+' or '-' as first sign of timezone
        int plusIndex = dateString.lastIndexOf('+');
        // int minusIndex = dateString.lastIndexOf('-');

        // Delete timezone
        if (plusIndex != -1) {
            return dateString.substring(0, plusIndex);
        } 
        // else if (minusIndex != -1) {
        //     return dateString.substring(0, minusIndex);
        // } 
        else {
            return dateString; 
        }
    }

    private String adjustTimezone(String date, String timezone) {
        // Parsing string to ZonedDateTime
        ZonedDateTime zonedDateTime = ZonedDateTime.parse(date, DateTimeFormatter.ISO_ZONED_DATE_TIME);

        // Convert to another timezone
        ZonedDateTime convertedTime = zonedDateTime.withZoneSameInstant(ZoneId.of(timezone));

        // Adjust the result without timezone name
        return convertedTime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    }

    private String standarizeDatetime(String date) {

        // Lowercase date
        date = date.toLowerCase();

        // Handle WIB, WITA, WIT timezones
        if (date.contains("wib")) {
            date = date.replace("wib", " ");
        } else if (date.contains("wita")) {
            date = date.replace("wita", " ");
        } else if (date.contains("wit")) {
            date = date.replace("wit", " ");
        }

        date = date.replace(" ", "T");

        // Change timezone to UTC+7
        if(!date.contains("+07")) {
            date = adjustTimezone(date, "Asia/Jakarta");
        }

        // Remove timezone
        date = removeTimezone(date);

        return date;
    }

    public String cleanAuthor(String author) {
        // Finding " - "
        int separatorIndex = author.indexOf(" - ");
        if (separatorIndex != -1) {
            return author.substring(0, separatorIndex);
        } else {
            return author; // If " - " doesn't exist, return original string
        }
    }

    public String[] convertAuthorToArray(String author) {
        if (author.contains(",")) {
            // Split author seperated by comma
            return author.split(",\\s*");
        } else {
            // Return array with single element
            return new String[] { author };
        }
    }
    
    
    // Create JSON for Kafka Content
    private JSONObject createJsonKafkaContent(String url, String domain, String content, String imageSource,
            String publishedDate, String title, String[] author) {
        JSONObject json = new JSONObject();
        json.put("url", url);
        json.put("domain", domain);
        json.put("content", content);
        json.put("last_checked", LocalDateTime.now());
        json.put("last_checked_ts", System.currentTimeMillis());
        json.put("image", imageSource);
        json.put("publishedDate", publishedDate);
        json.put("title", title);
        json.put("author", new JSONArray(author));
        return json;
    }

    // Create JSON Url
    private JSONObject createJsonKafkaUrl(String url, String landingUrl, String originalDomain, String domain, int depth,
            String urlSelect, String contentSelect) throws IOException {
        Integer maxDepth = crawlMediaRepository.getByDomain(domain).getMaxDepth();
        JSONObject json = new JSONObject();
        json.put("url", url);
        json.put("landing_url", landingUrl);
        json.put("original_domain", originalDomain);
        json.put("last_checked", LocalDateTime.now().toString());
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
    public String parseFilterSelector(List<String> filters, String baseSelector) {
        String selector = baseSelector;
        for (String string : filters) {
            selector +=  ":not(:contains(" + string + "))";
        }
        return selector;
    }

    public String getJSONMetadataAndRemoveHTMLTag(Document document) {
        String selectedScript = "";
        Elements elementsScript = document.select("script[type=application/ld+json]");
        for (Element elementScr : elementsScript) {
            if (elementScr.toString().contains("NewsArticle") || elementScr.toString().contains("Article"))
                selectedScript = elementScr.toString();
        }

        // Remove HTML tag
        return selectedScript = selectedScript.replaceAll("(?i)<script[^>]*>", " ").replaceAll("\\s+", " ")
                            .replace("</script>", "").trim();
    }

    public List<NewsArticle> parseNewsArticle(String jsonString) {
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

    private NewsArticle parseJSONObject(JSONObject jsonObject) {
        NewsArticle newsArticle = new NewsArticle();
        if (jsonObject.has("@type") && ("NewsArticle".equals(jsonObject.getString("@type"))  || "Article".equals(jsonObject.getString("@type"))) ) {
            newsArticle.setTitle(jsonObject.optString("headline"));

            // Check for "publishedDate" first, if not found, use "datePublished"
            if (jsonObject.has("publishedDate")) {
                newsArticle.setPublishedDate(jsonObject.optString("publishedDate"));
            } else {
                newsArticle.setPublishedDate(jsonObject.optString("datePublished"));
            }
            return newsArticle;
        }
        return null;
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

    public String getTitle(NewsArticle newsArticle) {
        return newsArticle.getTitle();
    }

    public String getPublishedDate(NewsArticle newsArticle) {
        return newsArticle.getPublishedDate();
    }

    public String parseImage(String url, String selector) throws IOException {
        Element element = getElements(url, selector).first();
        if (element != null) {
            String[] attributes = {"data-src", "src"};
            for (String attribute : attributes) {
                String attrValue = element.attr(attribute);
                if (!attrValue.isEmpty()) {
                    return attrValue;
                }
            }
        }
        return "";
    }

    public String parseAuthor(String url, String selector) throws IOException {
        Element element = getElements(url, selector).first();
        if(element != null) {
            return element.text();
        }
        return "";
    }

    private String unescapeHTMLSpecialCharacter(String textToUnescape) {
        return StringEscapeUtils.unescapeHtml4(textToUnescape);
    }

}

