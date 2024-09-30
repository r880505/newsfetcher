package id.labs247.medan.newsfetcher;

import static spark.Spark.port;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import id.labs247.medan.newsfetcher.configs.ConfigurationLoader;
import id.labs247.medan.newsfetcher.controllers.BackdateController;
import id.labs247.medan.newsfetcher.repositories.CrawlMediaRepository;
import id.labs247.medan.newsfetcher.scraper.CrawlerScheduler;
import id.labs247.medan.newsfetcher.scraper.NewsScraper;
import id.labs247.medan.newsfetcher.scraper.SolrService;

public class MainApplication {

    private static final Logger logger = LogManager.getLogger(MainApplication.class);
    private static ScheduledExecutorService restartScheduler = Executors.newSingleThreadScheduledExecutor();
    private static CrawlerScheduler crawlScheduler;
    private static Integer port = ConfigurationLoader.getPort();

    public static void main(String[] args) throws Exception {

        printBanner();
        logProperties();

        // Initialize Java Spark Controller
        port(port);
        new BackdateController();

        // Initialize each service
        CrawlMediaRepository crawlMediaDAO = new CrawlMediaRepository();
        NewsScraper newsScraper = new NewsScraper();
        SolrService solrService = new SolrService();
        solrService.init();
        crawlScheduler = new CrawlerScheduler(crawlMediaDAO, newsScraper);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            crawlScheduler.shutdown();
            logger.info("[INFO] Scheduler service shutdown.");
        }));

        // Start the crawler scheduler
        startScheduler();

        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void startScheduler() {
        try {
            crawlScheduler.init();
            logger.info("[INFO] CrawlerScheduler started successfully.");

            // Call the scheduler for refresh refresh crawl media data
            crawlScheduler.refreshCrawlMedia();
            logger.info("[INFO] refreshCrawlMedia scheduled successfully.");
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void restartSchedulerAfterDelay() {
        long initialDelay = computeInitialDelay(9, 5); // Schedule for 9 AM
        long period = TimeUnit.DAYS.toMinutes(1); // Run every 24 hours

        restartScheduler.scheduleAtFixedRate(() -> {
            try {
                logger.info("Shuting down the scheduler...");
                crawlScheduler.shutdown();

                // delayed for 10 minutes
                Thread.sleep(TimeUnit.MINUTES.toMillis(10));

                startScheduler();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, initialDelay, period, TimeUnit.MINUTES);
    }

    private static long computeInitialDelay(int targetHour, int targetMinute) {
        LocalDateTime now = LocalDateTime.now();
        ZonedDateTime nextRun = ZonedDateTime.of(now.toLocalDate(), now.toLocalTime(), ZoneId.systemDefault())
                .withHour(targetHour)
                .withMinute(targetMinute)
                .withSecond(0)
                .withNano(0);

        if (nextRun.isBefore(ZonedDateTime.now())) {
            nextRun = nextRun.plusDays(1); // Schedule for the next day if current time is past the target time
        }

        return TimeUnit.SECONDS.convert(java.time.Duration.between(ZonedDateTime.now(), nextRun).getSeconds(), TimeUnit.SECONDS);
    }

    private static void printBanner() throws ConfigurationException {
        String banner =
                "  __  __          _    _          ____  _  _ _____ \n" +
                " |  \\/  | ___  __| |  / \\   _ __ |___ \\| || |___  |\n" +
                " | |\\/| |/ _ \\/ _` | / _ \\ | '_ \\  __) | || |_ / / \n" +
                " | |  | |  __/ (_| |/ ___ \\| | | |/ __/|__   _/ /  \n" +
                " |_|  |_|\\___|\\__,_/_/   \\_\\_| |_|_____|  |_|/_/   \n";
        System.out.println("===================================================");
        System.out.println(banner);
        System.out.println("===================================================");
        logger.info("MedAn247-NewsFetcher --- The program is running");
    }

    private static void logProperties() {
        logger.info("Datasource URL: " + ConfigurationLoader.getString("datasource.url"));
        logger.info("Kafka Bootstrap Servers: " + ConfigurationLoader.getString("kafka.bootstrap.servers"));
        if(ConfigurationLoader.getString("solr.host")==null) {
            logger.info("Solr zk-host: " + ConfigurationLoader.getString("solr.zk-host"));
        } else {
            logger.info("Solr Host: " + ConfigurationLoader.getString("solr.host"));
        }
    }

}
