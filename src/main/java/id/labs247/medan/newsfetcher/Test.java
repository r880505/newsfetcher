package id.labs247.medan.newsfetcher;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import com.jayway.jsonpath.JsonPath;

import id.labs247.medan.newsfetcher.models.CrawlExtraComment;
import id.labs247.medan.newsfetcher.repositories.CrawlMediaRepository;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class Test {


    private static final String userAgentClientHints = "\"Chromium\";v=\"130\", \"Google Chrome\";v=\"130\", \"Not?A_Brand\";v=\"99\"";

    private static final String userAgent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";

    private static CrawlMediaRepository crawlMediaRepository = new CrawlMediaRepository();

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

    public static void main(String[] args) throws IOException {

        String datePublished =  "2024-11-15T20:17+0000";
        datePublished = parseDatetimeToDateOnly(datePublished);

        System.out.println(datePublished);

        // // Define domain, url and find mediaId by domain
        // String domain = "fajar.co.id";
        // Long mediaId = crawlMediaRepository.getNewsPortalByDomain(domain).getMediaId();
        // String url = "https://fajar.co.id/2024/10/22/said-didu-fufufafa-bukan-ban-serep-dan-langsung-menyalib-presiden-di-hari-kerja-pertama/";

        // // Connect to url using Jsoup
        // Document document = Jsoup.connect(url).userAgent(userAgent).get();

        // // Get data from database
        // CrawlExtraComment crawlExtraComment = crawlMediaRepository.getCrawlExtraCommentByMediaId(mediaId);
        // String commentApi = crawlExtraComment.getCommentApi();
        // String articleIdSelect = crawlExtraComment.getArticleIdSelect();
        // String cookie = crawlExtraComment.getCookie();
        // String articleId = parseArticleId(document, articleIdSelect);
        // String requestMethod = crawlExtraComment.getRequestMethod();
        // String selector = crawlExtraComment.getSelector();

        // // Complete request body or request param with articleId
        // String requestParam = completeRequestBodyOrParam(crawlExtraComment.getRequestParam(), articleId);
        // String requestBody = completeRequestBodyOrParam(crawlExtraComment.getRequestBody(), articleId);

        // // Parse comment
        // parseComment(domain, url, commentApi, requestMethod, requestBody, requestParam, cookie, selector);

    }


    private static JSONObject parseComment(String domain, String url, String commentApi, String requestMethod, String requestBody, String requestParam, String cookie, String selector) {

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

        JSONObject result = new JSONObject();

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
            
                    // Create array for result
                    JSONArray jsonArrayComment = new JSONArray();

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

                    result.put("comments", jsonArrayComment);
                    result.put("totalComments", jsonArrayComment.length());
                    result.put("url", url);
            
                    System.out.println(result.toString(2));
            
                } else {
                    System.err.println("Request failed: " + response.code());
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            System.err.println("No request sent, please check each parameter, request body or header");
        }

        return result;
        
    }

    private static String completeRequestBodyOrParam(String requestString, String articleId) {
        return requestString.replace("articleId", articleId);
    }

    private static String parseArticleId(Document document, String selector) {
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

    private static Long parseTimestamp(String timestampString) {
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

    private static String encodeUrl(String url) {
        try {
            return URLEncoder.encode(url, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return null;
        }
    }


    private static String parseDatetimeToDateOnly(String dateString) {
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
    
}
