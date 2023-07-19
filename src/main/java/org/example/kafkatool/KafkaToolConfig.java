package org.example.kafkatool;

import org.apache.commons.lang3.tuple.MutableTriple;
import org.apache.commons.lang3.tuple.Triple;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import static org.example.PrintUtil.printError;
import static org.example.PrintUtil.printInfo;

public class KafkaToolConfig {
    private static final Map<String, MutableTriple<Class<?>, Object, String>> CONFIG_MAP = new TreeMap<>();

    static {
        CONFIG_MAP.put("listConsoleConsumer", new MutableTriple<>(Boolean.class, false, "group命令列出consoleconsumer"));
    }

    public static <T> T getConfig(String config) {
        if (!CONFIG_MAP.containsKey(config)) {
            return null;
        }
        return (T) CONFIG_MAP.get(config).getMiddle();
    }

    public static void setConfig(String key, String value) {
        if (!CONFIG_MAP.containsKey(key)) {
            printInfo("no config named : '" + value + "'");
            return;
        }
        MutableTriple<Class<?>, Object, String> triple = CONFIG_MAP.get(key);
        Class<?> clazz = triple.getLeft();
        Object before = triple.getMiddle();
        if (clazz == Boolean.class) {
            triple.setMiddle(Boolean.parseBoolean(value));
        } else if (clazz == String.class) {
            triple.setMiddle(value.trim());
        } else if (clazz == Integer.class) {
            triple.setMiddle(Integer.parseInt(value));
        } else if (clazz == Long.class) {
            triple.setMiddle(Long.parseLong(value));
        } else {
            printError("not support type : " + clazz.getName());
            return;
        }
        printInfo(String.format("Before: %s \nAfter: %s", before, triple.getMiddle()));
    }

    public static void printAll() {
        String format = "%-20s%-20s%-10s%s";
        printInfo(String.format(format, "VARIABLE_NAME", "VARIABLE_TYPE", "VALUE", "DESC"));
       CONFIG_MAP.forEach((k,v)->{
           printInfo(String.format(format, k, v.getLeft().getName(), v.getMiddle(), v.getRight()));
       });
    }
}
