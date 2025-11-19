package id.labs247.medan.newsfetcher.controllers;

import static spark.Spark.*;
import java.util.*;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import id.labs247.medan.newsfetcher.models.CrawlMedia;
import id.labs247.medan.newsfetcher.repositories.CrawlMediaRepository;
import id.labs247.medan.newsfetcher.scraper.NewsScraper;

public class Controller {

    private static final Logger logger = LogManager.getLogger(Controller.class);

    private CrawlMediaRepository crawlMediaRepository = new CrawlMediaRepository();

    private NewsScraper newsScraper = new NewsScraper();

    private String baseUrl = "/newsfetcher/api";
    
    public Controller() {
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
            /*
            Example request body for /backdatenews endpoint:

            {
                "date": "2024-06-01",
                "page": 10,
                "domains": [
                    "detik.com",
                    "kompas.com"
                ]
            }
            */
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
                            newsScraper.executeParseIndexPage(domain, date, topicUrl, page);
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

        get(baseUrl + "/logo", (req, res) -> {
            logger.info("Received /logo request");

            String logoBasePath = System.getenv("LOGO_PATH");
            if (logoBasePath == null || logoBasePath.isEmpty()) {
                logger.error("LOGO_PATH environment variable is not set");
                res.status(500);
                return Response(500, "LOGO_PATH not set", null);
            }

            String domain = req.queryParams("domain");

            if (domain != null && !domain.isEmpty()) {
                // === Get logo by domain ===
                CrawlMedia crawlMedia = crawlMediaRepository.getNewsPortalByDomain(domain);
                if (crawlMedia == null) {
                    logger.warn("Domain not found: {}", domain);
                    res.status(404);
                    return Response(404, "Domain not found", null);
                }

                Map<String, Object> logoData = buildLogoData(logoBasePath, crawlMedia);
                if (logoData == null) {
                    res.status(404);
                    return Response(404, "Logo file not found", null);
                }

                res.type("application/json");
                res.status(200);
                return Response(200, "Successfully get logo", logoData);

            } else {
                // === Get all logos ===
                List<CrawlMedia> allMedia = crawlMediaRepository.getAllActiveNewsPortal();
                List<Map<String, Object>> allData = new ArrayList<>();

                for (CrawlMedia media : allMedia) {
                    Map<String, Object> logoData = buildLogoData(logoBasePath, media);
                    if (logoData != null) {
                        allData.add(logoData);
                    }
                }

                res.type("application/json");
                res.status(200);
                return Response(200, "Successfully get all logos", allData);
            }
        });


        get(baseUrl + "/all-newsportal", (req, res) -> {
            res.type("application/json");

            String logoBasePath = System.getenv("LOGO_PATH");
            if (logoBasePath == null || logoBasePath.isEmpty()) {
            logger.error("LOGO_PATH environment variable is not set");
            res.status(500);
            return Response(500, "LOGO_PATH not set", null);
            }

            try {
                List<CrawlMedia> allMedia = crawlMediaRepository.getAllNewsPortal();
                List<Map<String, Object>> allData = allMedia.stream().map(media -> {
                    Map<String, Object> dataPortal = new HashMap<>();
                    dataPortal.put("domain", media.getOriginalDomain());
                    dataPortal.put("landingPage", media.getLandingUrl());
                    dataPortal.put("logoBase64", getLogoBase64(logoBasePath, media));
                    
                    Boolean isActive = media.getScheduleMinutes() >= 0 ? true : false;
                    dataPortal.put("isActive", isActive);

                    LocalDateTime lastScheduled = media.getLastScheduled();
                    if (lastScheduled != null) {
                        ZoneId zonePlus7 = ZoneId.of("Asia/Jakarta");
                        ZonedDateTime zonedDateTime = lastScheduled.atZone(zonePlus7);
                        ZonedDateTime utcDateTime = zonedDateTime.withZoneSameInstant(ZoneOffset.UTC);
                        dataPortal.put("lastScheduled", utcDateTime.toString());
                    } else {
                        dataPortal.put("lastScheduled", null);
                    }

                    return dataPortal;
                }).collect(Collectors.toList());

                res.status(200);
                return Response(200, "Successfully get data", allData);
            } catch (Exception e) {
                logger.error("Failed to get all news portals", e);
                res.status(500);
                return Response(500, "Internal server error", null);
            }
        });        get(baseUrl + "/get-selector", (req, res) -> {
            res.type("application/json");
            try {
                CrawlMedia crawlMedia = crawlMediaRepository.getNewsPortalByDomain("antaranews.com");
                res.status(200);
                return Response(200, "Successfully get selector", crawlMedia.getContentSelect());
            } catch (Exception e) {
                res.status(500);
                return Response(500, "Internal server error", null);
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

    private Map<String, Object> buildLogoData(String logoBasePath, CrawlMedia media) {
            String logoBase64 = getLogoBase64(logoBasePath, media);

            Map<String, Object> data = new HashMap<>();
            data.put("domain", media.getOriginalDomain());
            data.put("landingPage", media.getLandingUrl());
            data.put("logoBase64", logoBase64);

            return data;
    }

    private String getLogoBase64(String logoBasePath, CrawlMedia media) {
        File logoFile = new File(logoBasePath, media.getMediaLogo());

        if (!logoFile.exists()) {
            logger.warn("Logo file not found: {}", logoFile.getAbsolutePath());
            return null;
        }

        try {
            byte[] fileBytes = Files.readAllBytes(logoFile.toPath());
            String mimeType = Files.probeContentType(logoFile.toPath());
            if (mimeType == null) mimeType = "application/octet-stream";

            String base64 = Base64.getEncoder().encodeToString(fileBytes);
            return "data:" + mimeType + ";base64," + base64;
        } catch (IOException e) {
            logger.warn("Failed to read logo file: {}", e.getMessage());
            return null;
        }
    }

}
