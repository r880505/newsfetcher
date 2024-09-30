package id.labs247.medan.newsfetcher.scraper;

import id.labs247.medan.newsfetcher.models.CrawlMedia;
import id.labs247.medan.newsfetcher.repositories.CrawlMediaRepository;
import id.labs247.medan.newsfetcher.utils.ThreadPoolUtil;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CrawlerScheduler {

    private final CrawlMediaRepository crawlMediaDAO;
    private final NewsScraper newsScraper;
    private ScheduledExecutorService scheduler;
    private ExecutorService asyncTaskExecutor;
    private static final Logger logger = LogManager.getLogger(CrawlerScheduler.class);
    private List<CrawlMedia> allCrawlMedia;

    public CrawlerScheduler(CrawlMediaRepository crawlMediaDAO, NewsScraper newsScraper) {
        this.crawlMediaDAO = crawlMediaDAO;
        this.newsScraper = newsScraper;
        this.scheduler = Executors.newScheduledThreadPool(1000);
        this.asyncTaskExecutor = ThreadPoolUtil.createAsyncTaskExecutor();
    }

    public void init() throws IOException {
        scheduleTasks();
    }

    public void shutdown() {
        if (scheduler != null) {
            scheduler.shutdown();
        }
        if (asyncTaskExecutor != null) {
            asyncTaskExecutor.shutdown();
        }
    }

    public void scheduleTasks() throws IOException {
        allCrawlMedia = crawlMediaDAO.getAllActivePortal();
        for (CrawlMedia crawlMedia : allCrawlMedia) {
            int scheduleMinute = crawlMedia.getScheduleMinutes();
            long initialDelay = getInitialDelay(scheduleMinute);

            scheduler.scheduleAtFixedRate(() -> {
                try {
                    runCrawlingTask(crawlMedia);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }, initialDelay, TimeUnit.HOURS.toMinutes(1), TimeUnit.MINUTES);
        }
    }

    private long getInitialDelay(int scheduleMinute) {
        LocalDateTime now = LocalDateTime.now();
        int currentMinute = now.getMinute();
        int delay = (scheduleMinute - currentMinute + 60) % 60;
        return delay;
    }

    public void runCrawlingTask(CrawlMedia crawlMedia) {
        asyncTaskExecutor.submit(() -> {
            try {
                // Schedule index page parsing immediately
                LocalDateTime localDateTime = LocalDateTime.now();
                String domain = crawlMedia.getOriginalDomain();
                String topicUrl = newsScraper.getTopicUrl(domain);
                String topicNews = newsScraper.getTopicContent(domain);
                String topicRelatedNewsUrl = newsScraper.getTopicBacaJuga(domain);
                String dateToParse = newsScraper.dateFormatter(localDateTime, "yyyy-MM-dd");

                // Schedule index page parsing immediately
                int page = crawlMedia.getIndexPageCount();
                newsScraper.executeParseIndexPage(domain, dateToParse, topicUrl, page);
                logger.info("[DEBUG] | Completed scraping index page for domain: " + crawlMedia.getOriginalDomain());

                crawlMedia.setLastScheduled(localDateTime);
                crawlMediaDAO.update(crawlMedia);

                // Schedule parsing news immediately
                scheduler.schedule(() -> {
                    try {
                        newsScraper.executeParseNews(domain, topicUrl, topicNews, topicRelatedNewsUrl);
                        newsScraper.insertToSolr(topicNews);
                        logger.info("[DEBUG] | Completed scraping news for domain: " + crawlMedia.getOriginalDomain());

                        // Schedule parsing related news immediately after news parsing
                        scheduler.schedule(() -> {
                            try {
                                newsScraper.executeParseRelatedNews(domain, topicUrl, topicNews, topicRelatedNewsUrl);
                                newsScraper.insertToSolr(topicNews);
                                logger.info("[DEBUG] | Completed scraping related news for domain: " + crawlMedia.getOriginalDomain());
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }, 0, TimeUnit.MINUTES);

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }, 0, TimeUnit.MINUTES);

            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    public void refreshCrawlMedia() {
        long initialDelay = getInitialDelay(50);  // Calculate the delay for 50th minute
        scheduler.scheduleAtFixedRate(() -> {
            try {
                allCrawlMedia = crawlMediaDAO.getAllActivePortal();  // Fetch active portals
                for (CrawlMedia crawlMedia : allCrawlMedia) {
                    // Check if lastScheduled is the default value
                    if (crawlMedia.getLastScheduled() != null && crawlMedia.getLastScheduled().equals(LocalDateTime.of(2024, 1, 1, 10, 0))) {
                        logger.info("[INFO] Added new portal: {}", crawlMedia.getOriginalDomain());
                    }
                }
                logger.info("[INFO] Updated CrawlMedia list at {}", LocalDateTime.now());
            } catch (IOException e) {
                logger.error("Failed to fetch the crawl media data: " + e.getMessage(), e);  // Enhanced error logging
                e.printStackTrace();
            }
        }, initialDelay, TimeUnit.HOURS.toMinutes(1), TimeUnit.MINUTES);  // Execute every 1 hour
    }

}
