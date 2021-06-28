package com.zjtd.utils;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPoolUtil {

    //声明线程池
    private static ThreadPoolExecutor threadPoolExecutor;

    //私有化构造方法
    private ThreadPoolUtil() {
    }

    public static ThreadPoolExecutor getThreadPoolExecutor() {

        if (threadPoolExecutor == null) {
            synchronized (ThreadPoolUtil.class) {
                if (threadPoolExecutor == null) {
                    threadPoolExecutor = new ThreadPoolExecutor(4,
                            20,
                            600L,
                            TimeUnit.SECONDS,
                            new LinkedBlockingDeque<>());
                }
            }
        }

        return threadPoolExecutor;
    }

}
