package id.labs247.medan.newsfetcher;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
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
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss'Z'")      
    );

    public static void main(String[] args) throws IOException {

        // Define domain, url and find mediaId by domain
        String domain = "kompas.com";
        Long mediaId = crawlMediaRepository.getNewsPortalByDomain(domain).getMediaId();
        String url = "https://nasional.kompas.com/read/2024/09/13/19340911/soal-heboh-akun-fufufafa-elite-golkar-itu-ingin-picu-perpecahan";

        // Connect to url using Jsoup
        Document document = Jsoup.connect(url).userAgent(userAgent).get();

        // Get data from database
        CrawlExtraComment crawlExtraComment = crawlMediaRepository.getCrawlExtraCommentByMediaId(mediaId);
        String commentApi = crawlExtraComment.getCommentApi();
        String articleIdSelect = crawlExtraComment.getArticleIdSelect();
        String cookie = crawlExtraComment.getCookie();
        String articleId = parseArticleId(document, articleIdSelect);
        String requestMethod = crawlExtraComment.getRequestMethod();
        String selector = crawlExtraComment.getSelector();

        // Complete request body or request param with articleId
        String requestParam = completeRequestBodyOrParam(crawlExtraComment.getRequestParam(), articleId);
        String requestBody = completeRequestBodyOrParam(crawlExtraComment.getRequestBody(), articleId);

        // Parse comment
        parseComment(domain, url, commentApi, requestMethod, requestBody, requestParam, cookie, selector);

    }


    private static void parseComment(String domain, String url, String commentApi, String requestMethod, String requestBody, String requestParam, String cookie, String selector) {

        // Initiate okhttp
        OkHttpClient client = new OkHttpClient();

        // Create request with null value
        Request request = null;

        // Complete request to comment API with different case between 'POST' and 'GET'
        if("POST".equals(requestMethod)) {
            RequestBody body = RequestBody.create(MediaType.get("application/json"), requestBody);
            request = new Request.Builder()
                .url(commentApi)
                .post(body)
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
        } 

        if("GET".equals(requestMethod)) {
            request = new Request.Builder()
                .url(commentApi+requestParam)
                .get()
                .build();
        }

        // Send request and het the response
        if(request!=null) {
            // Send request
            try (Response response = client.newCall(request).execute()) {
                if (response.isSuccessful()) {
                    // Get response body
                    String responseBody = response.body().string();
            
                    // Load selector JSON from database
                    JSONObject jsonSelector = new JSONObject(selector);
            
                    // Ambil jsonPath dan selector
                    String jsonPath = jsonSelector.getString("jsonPath");
                    JSONArray selectorArray = jsonSelector.getJSONArray("selector");
            
                    // Ekstrak daftar komentar berdasarkan jsonPath
                    List<Map<String, Object>> commentList = JsonPath.parse(responseBody).read(jsonPath);

                    System.out.println(commentList);
            
                    // Siapkan array hasil akhir
                    JSONArray jsonArrayComment = new JSONArray();
            
                    // Loop setiap objek komentar
                    for (Map<String, Object> obj : commentList) {
                        JSONObject jsonCommentResult = new JSONObject();

                        Integer like = 0;
                        Integer dislike = 0;
            
                        // Iterasi setiap field di dalam selector
                        for (int i = 0; i < selectorArray.length(); i++) {
                            JSONObject field = selectorArray.getJSONObject(i);
                            String fieldName = field.keys().next(); // Nama field seperti "name", "comment", dll.
                            JSONObject fieldDetails = field.getJSONObject(fieldName);
            
                            // Ambil tipe dan path untuk field ini
                            String fieldType = fieldDetails.getString("type");
                            String fieldPath = fieldDetails.optString("path", null);
            
                            // Parsing sesuai tipe data
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
                                    case "StringDateTime": // Sesuaikan jika ingin DateTime diparsing sebagai String
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

                                // Ambil nilai 'text' dari JSON string yang ada di dalam komentar
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
            
                            // Jika value null, berikan default untuk 'like' dan 'dislike'
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

                    JSONObject result = new JSONObject();
                    result.put("comments", jsonArrayComment);
                    result.put("totalComments", jsonArrayComment.length());
                    result.put("url", url);
            
                    // Print hasil akhir
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
    

}
