package id.labs247.medan.newsfetcher.utils;

import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.jsoup.nodes.Document;

import id.labs247.medan.newsfetcher.models.NewsArticle;

public class JsonHelper {

    public static String sanitizeJSON(String jsonString) {
        return jsonString.replaceAll("[\n\r]+", "");
    }

    public static List<NewsArticle> parseNewsArticles(String jsonString) {
        jsonString = sanitizeJSON(jsonString);
        Object json = new JSONTokener(jsonString).nextValue();
        List<NewsArticle> newsArticles = new ArrayList<>();
    
        if (json instanceof JSONObject) {
            JSONObject jsonObject = (JSONObject) json;
            if (jsonObject.has("@graph")) {
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

    public static NewsArticle parseJSONObject(JSONObject jsonObject) {
        NewsArticle newsArticle = new NewsArticle();
        if (jsonObject.has("@type") && ("NewsArticle".equals(jsonObject.getString("@type"))  || "Article".equals(jsonObject.getString("@type"))) ) {
            newsArticle.setTitle(jsonObject.optString("headline"));
            String datePublished = jsonObject.has("datePublished") ? jsonObject.optString("datePublished") : jsonObject.optString("publishedDate");
            if(DateUtils.matchFormatter(datePublished)==false) {
                datePublished = jsonObject.optString("dateModified");
            }   
            newsArticle.setDatePublished(datePublished);
            if (jsonObject.has("author")) {
                newsArticle.setAuthor(parseAuthorFromJSON(jsonObject.get("author")));
            } else if (jsonObject.has("authors")) {
                newsArticle.setAuthor(parseAuthorFromJSON(jsonObject.get("authors")));
            }

            return newsArticle;
        }
        return null;
    }

    public static List<NewsArticle> parseJSONArray(JSONArray jsonArray) {
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

    public static List<NewsArticle> getNewsArticles(Document document) {
        String jsonPropOfNews = JsoupHelper.getJSONMetadataAndRemoveHTMLTag(document);
        return parseNewsArticles(jsonPropOfNews);
    }

    public static List<String> parseAuthorFromJSON(Object authorObject) {
        List<String> authors = new ArrayList<>();
    
        if (authorObject instanceof JSONObject) {
            JSONObject author = (JSONObject) authorObject;
            if (author.has("name")) {
                authors.add(author.optString("name", ""));
            }
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
}
