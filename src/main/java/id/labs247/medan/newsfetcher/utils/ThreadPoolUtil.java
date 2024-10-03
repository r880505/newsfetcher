package id.labs247.medan.newsfetcher.utils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ThreadPoolUtil {

    private static final Logger logger = LogManager.getLogger(ThreadPoolUtil.class);

    public static ExecutorService createAsyncTaskExecutor() {
        int corePoolSize =100;
        int maximumPoolSize = 2000;
        long keepAliveTime = 60L;
        int queueCapacity = 1000;

        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                corePoolSize,
                maximumPoolSize,
                keepAliveTime,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(queueCapacity),
                Executors.defaultThreadFactory(),
                (Runnable r, ThreadPoolExecutor e) -> {
                    logger.info("Task Rejected: Thread pool is full. Increase the thread pool size.");
                }
        );

        return executor;
    }
}

