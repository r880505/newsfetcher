package id.labs247.medan.newsfetcher.controllers;

import static spark.Spark.*;
import java.util.*;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import java.time.LocalDateTime;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import id.labs247.medan.newsfetcher.repositories.CrawlMediaRepository;
import id.labs247.medan.newsfetcher.scraper.NewsScraper;

public class BackdateController {

    private static final Logger logger = LogManager.getLogger(BackdateController.class);

    private CrawlMediaRepository crawlMediaRepository = new CrawlMediaRepository();

    private NewsScraper newsScraper = new NewsScraper();

    private String baseUrl = "/newsfetcher/api";
    
    public BackdateController() {
        setupRoutes();
    }

    private void setupRoutes() {


        // Get list of news portal that can be parse old news by date
        get(baseUrl+"/newsportal", (req, res) -> {
            res.type("application/json");
            try {
                List<String> domains = crawlMediaRepository.getBackdateNewsPortal();
                res.status(200);
                return Response(200, "Successfully get data", domains);
            } catch (Exception e) {
                res.status(500);
                return Response(500, "Internal server error", null);
            }
            
        });

        // Parse news portal by old date
        post(baseUrl+"/backdatenews", (req, res) -> {
            res.type("application/json");
            
            // Initialize ExecutorService
            ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
            LocalDateTime startTime = LocalDateTime.now();

            try {
                // Parse the request body (JSON) into a JSONObject
                JSONObject requestBody = new JSONObject(req.body());

                // Get date from request body
                String date = requestBody.optString("date");

                // Get page from request body
                final Integer page = requestBody.has("page") ? requestBody.getInt("page") : 20;

                // Get domains from request body
                List<String> domains = new ArrayList<>();
                int length = requestBody.getJSONArray("domains").length();
                for (int i = 0; i < length; i++) {
                    String domain = requestBody.getJSONArray("domains").get(i).toString();
                    domains.add(domain);
                }
                String domainsString = String.join(", ", domains);

                // Create a list of CompletableFuture tasks
                List<CompletableFuture<Void>> futures = domains.stream()
                    .map(domain -> CompletableFuture.runAsync(() -> {
                        try {
                            String topicUrl = newsScraper.getTopicUrlBackdate(domain);
                            String topicNews = newsScraper.getTopicContentBackdate(domain);
                            String topicBacaJuga = newsScraper.getTopicBacaJugaBackdate(domain);
                            newsScraper.parseIndexPage(domain, date, topicUrl, page);
                            newsScraper.parseNews(domain, false, topicUrl, topicNews, topicBacaJuga);
                            newsScraper.insertNewsToSolr(topicNews);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }, executorService))
                    .collect(Collectors.toList());

                
                // Continue processing in background
                CompletableFuture<Void> allOf = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
                allOf.thenRun(() -> {
                    // Log success message after all tasks are completed
                    logger.info(String.format("[DEBUG] Successfully scraped news from %s for date %s, start time %s, end time %s", 
                        domainsString, date, startTime, LocalDateTime.now()));
                    executorService.shutdown();
                }).exceptionally(ex -> {
                    ex.printStackTrace();
                    executorService.shutdown();
                    return null;
                });

                // Send response back
                res.status(200);
                return Response(200, String.format("Scraping news from %s for date %s has started at %s", 
                domainsString, date, startTime), null);

            } catch (Exception e) {
                // Handle exception and return error response
                res.status(500);
                return Response(500, "Failed scrape backdate", null);
            }
        });

    }

    private JSONObject Response(Integer statusCode, String message, Object data) {
        JSONObject response = new JSONObject();
        response.put("statusCode", statusCode);
        response.put("message",message);
        if(data!=null) {
            response.put("data", data);
        }
        return response;
    }
}
