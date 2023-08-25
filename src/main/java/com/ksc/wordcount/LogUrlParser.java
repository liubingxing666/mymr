package com.ksc.wordcount;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogUrlParser {

    public static List<String> extractUrlsFromFile(String filePath) {
        List<String> urls = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            String urlPattern = "http\\S+"; // URL的正则表达式模式

            Pattern pattern = Pattern.compile(urlPattern);

            while ((line = reader.readLine()) != null) {
                Matcher matcher = pattern.matcher(line);
                while (matcher.find()) {
                    String url = matcher.group();
                    url = url.replaceAll("[^a-zA-Z0-9]+$", "");
                    if (url.contains("\"")) {
                        String[] ss = url.split("\"");
                        String url22 = ss[0].replaceAll("[^a-zA-Z0-9]+$", "");
                        urls.add(url22);
                    } else {
                        urls.add(url.trim());
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return urls;
    }
    public static List<String> extractUrlsFromLine(String line) {
        List<String> urls = new ArrayList<>();
        String urlPattern = "http\\S+"; // URL的正则表达式模式

        Pattern pattern = Pattern.compile(urlPattern);
        Matcher matcher = pattern.matcher(line);

        while (matcher.find()) {
            String url = matcher.group();
            url = url.replaceAll("[^a-zA-Z0-9]+$", "");
            if (url.contains("\"")) {
                String[] ss = url.split("\"");
                String url22 = ss[0].replaceAll("[^a-zA-Z0-9]+$", "");
                urls.add(url22);
            } else {
                urls.add(url.trim());
            }
        }

        return urls;
    }



    public static Map<String,String> secondExtractUrlsFromLine(String line) {
       Map<String,String> urlSum =new HashMap<>();
        String strs[]=line.split("\\s");
        urlSum.put(strs[0],strs[1]);
        return urlSum;
    }
//    public static void main(String[] args) throws IOException {
//        String filePath = "C:/tmp/input/test1.log";
//        List<String> urls = extractUrlsFromFile(filePath);
//
//        for (String url : urls) {
//            System.out.println(url);
//        }
//    }

}