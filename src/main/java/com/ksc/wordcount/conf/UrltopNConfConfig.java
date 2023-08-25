package com.ksc.wordcount.conf;

public class UrltopNConfConfig {

    public static String inputPath = null;
    public static String outputPath = null;
    public static String applicationId = null;
    public static int reduceTaskNum = 2;
    public static  long size=0;
    public static  int topn=0;


    public static void getInfo(){
        System.out.println("inputPath: " + inputPath);
        System.out.println("outputPath: " + outputPath);
        System.out.println("applicationId: " +applicationId );
        System.out.println("reduceTaskNum: " + reduceTaskNum);
        System.out.println("size: " + size);
    }
}
