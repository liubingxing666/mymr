package com.ksc.wordcount.conf;


import lombok.Data;

@Data
public class MasterConfConfig {
    public static String host=null  ;
    public static Integer akkaPort=0 ;
    public static Integer thriftPort=0;
    public static String memory =null;


    public static void getInfo(){
        System.out.println("ip: " + host);
        System.out.println("akkaPort: " + akkaPort);
        System.out.println("thriftPort: " +thriftPort );
        System.out.println("memory: " + memory);
    }

}
