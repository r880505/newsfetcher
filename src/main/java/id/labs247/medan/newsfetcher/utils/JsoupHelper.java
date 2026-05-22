package id.labs247.medan.newsfetcher.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.text.StringEscapeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import id.labs247.medan.newsfetcher.models.NewsArticle;

public class JsoupHelper {
    private static final Logger logger = LogManager.getLogger(JsoupHelper.class);
    private static final String userAgent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";

    public static Document getDocument(String url) throws IOException {
        try {
            Connection.Response response = Jsoup.connect(url)
                    .userAgent(userAgent)
                    .referrer("https://www.google.com")
                    .execute();
            int statusCode = response.statusCode();
            if (statusCode != 200) {
                logger.error("[ERROR] | HTTP error: " + statusCode + " - " + response.statusMessage());
            }

            return response.parse();
        } catch (IOException e) {
            logger.error("[ERROR] Failed to connect to URL: " + url);
            throw e;
        }
    }

    public static Elements getElements(String url, String selector) throws IOException {
        return getElements(getDocument(url), selector);
    }

    public static Elements getElements(Document document, String selector) throws IOException {
        if (selector != null && !selector.isEmpty()) {
            return document.select(selector);
        } else {
            logger.error("[ERROR] CSS query must not be empty.");
            return new Elements(); // Return empty elements to avoid breaking execution
        }
    }

    public static String getNews(String url, String selector) throws IOException {
        return getNews(getDocument(url), selector);
    }

    public static String getNews(Document document, String selector) throws IOException {
        Elements paragraphs = getElements(document, selector);

        StringBuilder content = new StringBuilder();
        for (Element paragraph : paragraphs) {
            if (paragraph.text().toLowerCase().length() != 0) {
                content.append(paragraph.text().trim() + " ");
            }
        }
        String result = content.toString().trim();

        return result;
    }

    public static List<String> getUrl(String url, String selector) {
        try {
            return getUrl(getDocument(url), selector);
        } catch (IOException e) {
            logger.error("[ERROR] " + e.getMessage());
            return new ArrayList<>();
        }
    }

    public static List<String> getUrl(Document document, String selector) {
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

    public static String parseTitle(Document document) {
        List<NewsArticle> newsArticles = JsonHelper.getNewsArticles(document);
        String title = "";
        for (NewsArticle newsArticle : newsArticles) {
            title = unescapeHTMLSpecialCharacter(newsArticle.getTitle());
        }
        return title;
    }

    public static List<String> parseAuthor(Document document) {
        List<NewsArticle> newsArticles = JsonHelper.getNewsArticles(document);
        List<String> authors = new ArrayList<>();
        for (NewsArticle newsArticle : newsArticles) {
            List<String> articleAuthors = newsArticle.getAuthor();
            for (String author : articleAuthors) {
                authors.add(unescapeHTMLSpecialCharacter(author));
            }
        }
        return authors;
    }

    public static String parseDatePublished(Document document) {
        List<NewsArticle> newsArticles = JsonHelper.getNewsArticles(document);
        String datePublished = "";
        for (NewsArticle newsArticle : newsArticles) {
            datePublished = DateUtils.standarizeDatetime(newsArticle.getDatePublished(), "Asia/Jakarta");
        }
        return datePublished;
    }

    public static String parseImage(Document document, List<String> selectors) throws IOException {
        for (String selector : selectors) {
            Element element = getElements(document, selector).first();
            if (element != null) {
                String[] attributes = { "data-src", "src", "content" };
                for (String attribute : attributes) {
                    String attrValue = element.attr(attribute);
                    if (!attrValue.isEmpty()) {
                        return attrValue; // Return if any image found
                    }
                }
            }
        }
        return ""; // Return if image not found
    }

    public static String[] parseKeywords(Document document) {
        Element keywordsMetaTag = document.selectFirst("meta[name=keywords]");
        if (keywordsMetaTag != null) {
            String keywordsContent = keywordsMetaTag.attr("content");

            if (keywordsContent != null && !keywordsContent.trim().isEmpty()) {
                return keywordsContent.split(",\\s*");
            }
        }
        return new String[] {};
    }

    public static String getJSONMetadataAndRemoveHTMLTag(Document document) {
        String selectedScript = "";
        Elements elementsScript = document.select("script[type=application/ld+json]");
        for (Element elementScr : elementsScript) {
            String jsonContent = elementScr.html();
            if (jsonContent.contains("NewsArticle") || jsonContent.contains("Article") ||
                    jsonContent.contains("\"@type\":\"NewsArticle\"")
                    || jsonContent.contains("\"@type\":\"Article\"")) {
                selectedScript = jsonContent;
                break;
            }
        }
        return selectedScript.trim();
    }

    public static String parseArticleId(Document document, String selector) {
        try {
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

    public static String unescapeHTMLSpecialCharacter(String textToUnescape) {
        return StringEscapeUtils.unescapeHtml4(textToUnescape);
    }
}
