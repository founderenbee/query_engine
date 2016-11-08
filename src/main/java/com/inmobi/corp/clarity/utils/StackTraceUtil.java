package com.inmobi.corp.clarity.utils;

import java.io.PrintWriter;
import java.io.StringWriter;

public class StackTraceUtil {
    public static String stackTraceToString(Throwable tw){
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        tw.printStackTrace(pw);
        return sw.toString();
    }
}
