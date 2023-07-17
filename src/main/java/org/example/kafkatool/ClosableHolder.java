package org.example.kafkatool;

import org.example.PrintUtil;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

public class ClosableHolder {
    Map<String, Runnable> closableMap = new ConcurrentHashMap<>();

    /**
     * 替换已有任务，并返回被替换的任务
     * @param key
     * @param runnable
     * @return
     */
    public Runnable replaceTask(String key, Runnable runnable) {
        return closableMap.replace(key, runnable);
    }

    /**
     * 如果不存在同名任务则注册，已存在则跳过
     * @param key
     * @param runnable
     */
    public void registerIfNotExists(String key, Runnable runnable) {
        closableMap.putIfAbsent(key, runnable);
    }

    public void cleanUp() {
        closableMap.values().forEach(e -> {
            try {
                e.run();
            } catch (Exception exception) {
                PrintUtil.printError(exception);
            }
        });
    }
}
