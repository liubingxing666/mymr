package com.ksc.wordcount;


import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class GenerateData {
    private static final String[] IP_ADDRESSES = {"192.168.0.1", "192.168.0.2", "192.168.0.3", "192.168.0.4", "192.168.0.5"};
    private static final String[] METHODS = {"GET", "POST"};
    private static final String[] URLS = {
            "http://example.com/page1", "http://example.com/page2", "http://example.com/page3", "http://example.com/page4",
            "http://example.com/page5", "http://example.com/page6", "http://example.com/page7", "http://example.com/page8",
            "http://example.com/page9", "http://example.com/page10", "http://example.com/page11", "http://example.com/page12",
            "http://example.com/page13", "http://example.com/page14", "http://example.com/page15", "http://example.com/page16",
            "http://example.com/page17", "http://example.com/page18", "http://example.com/page19", "http://example.com/page20",
            "http://example.com/page21", "http://example.com/page22", "http://example.com/page23", "http://example.com/page24",
            "http://example.com/page25", "http://example.com/page26", "http://example.com/page27", "http://example.com/page28",
            "http://example.com/page29", "http://example.com/page30", "http://example.com/page31", "http://example.com/page32",
            "http://example.com/page33", "http://example.com/page34", "http://example.com/page35", "http://example.com/page36",
            "http://example.com/page37", "http://example.com/page38", "http://example.com/page39", "http://example.com/page40",
            "http://example.com/page41", "http://example.com/page42", "http://example.com/page43", "http://example.com/page44",
            "http://example.com/page45", "http://example.com/page46", "http://example.com/page47", "http://example.com/page48",
            "http://example.com/page49", "http://example.com/page50"
    };
    private static final int[] STATUSES = {200, 404, 500};

    public static void main(String[] args) {
        generateAndWriteDataToFile(2000, "C:/tmp/input/data.txt");
    }

    public static void generateAndWriteDataToFile(int numRows, String filename) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename))) {
            Random random = new Random();
            for (int i = 0; i < numRows; i++) {
                String ip = IP_ADDRESSES[random.nextInt(IP_ADDRESSES.length)];
                String timestamp = generateTimestamp();
                String method = METHODS[random.nextInt(METHODS.length)];
                String url = URLS[random.nextInt(URLS.length)];
                int status = STATUSES[random.nextInt(STATUSES.length)];

                String data = generateDataEntry(ip, timestamp, method, url, status);
                writer.write(data);
                writer.newLine();
            }
            System.out.println("数据已成功写入文件: " + filename);
        } catch (IOException e) {
            System.out.println("写入文件时出现错误: " + e.getMessage());
        }
    }

    private static String generateTimestamp() {
        long minTimestamp = 1678531200L; // 10/Aug/2023:00:00:00 in Unix timestamp
        long maxTimestamp = 1678617599L; // 10/Aug/2023:23:59:59 in Unix timestamp
        long randomTimestamp = minTimestamp + new Random().nextLong() % (maxTimestamp - minTimestamp + 1);
        return String.format("%tc", randomTimestamp * 1000);
    }

    private static String generateDataEntry(String ip, String timestamp, String method, String url, int status) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"IP\": \"").append(ip).append("\", ");
        sb.append("\"Timestamp\": \"").append(timestamp).append("\", ");
        sb.append("\"Method\": \"").append(method).append("\", ");
        sb.append("\"URL\": \"").append(url).append("\", ");
        sb.append("\"Status\": ").append(status).append("}");
        return sb.toString();
    }
}