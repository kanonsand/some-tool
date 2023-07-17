package org.example;

import java.io.PrintWriter;
import java.io.StringWriter;

public class PrintUtil {

    public static void main(String[] args) {
        System.out.println(String.format("%-15s", 100));
    }
    public static void printInfo(String s) {
        System.out.println(s);
    }

    public static void printPrefix(String s) {
        System.out.print(s);
    }

    public static void printError(String s) {
        System.err.println(s);

    }

    public static void printError(Throwable throwable) {
        try (StringWriter stringWriter = new StringWriter();
             PrintWriter printWriter = new PrintWriter(stringWriter)) {
            throwable.printStackTrace(printWriter);
            printError(stringWriter.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
