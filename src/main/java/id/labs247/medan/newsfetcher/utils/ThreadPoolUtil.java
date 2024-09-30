package id.labs247.medan.newsfetcher.utils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPoolUtil {

    public static ExecutorService createAsyncTaskExecutor() {
        int corePoolSize = 100;
        int maximumPoolSize = 1000;
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
                    System.out.println("Task Rejected: Thread pool is full. Increase the thread pool size.");
                }
        );

        return executor;
    }
}

