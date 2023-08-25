package com.ksc.wordcount.worker;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.ksc.wordcount.conf.MasterConfConfig;
import com.ksc.wordcount.conf.UrltopNConfConfig;
import com.ksc.wordcount.rpc.Executor.ExecutorActor;
import com.ksc.wordcount.rpc.Executor.ExecutorRpc;
import com.ksc.wordcount.rpc.Executor.ExecutorSystem;
import com.ksc.wordcount.rpc.ExecutorRegister;
import com.ksc.wordcount.shuffle.nettyimpl.server.ShuffleService;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class Executor {
    public static void main(String[] args) throws InterruptedException {
//        ExecutorEnv.host="192.168.1.151";
//        ExecutorEnv.host="192.168.1.196";
               // ExecutorEnv.port=15050;
//        ExecutorEnv.host="127.0.0.1";
        InitMasterCOnfConfig();
      //  InitUrlTopNConfConfig();

        ExecutorEnv.host=args[0];
        ExecutorEnv.port=Integer.parseInt(args[1]);
        ExecutorEnv.shufflePort=Integer.parseInt(args[2]) ;
        ExecutorEnv.memory=args[3];
        ExecutorEnv.core=Integer.parseInt(args[4].replaceAll("[^0-9]", "").trim()) ;


//        ExecutorEnv.memory="512m";
//        ExecutorEnv.driverUrl="akka.tcp://DriverSystem@192.168.1.151:4040/user/driverActor";
        ExecutorEnv.driverUrl="akka.tcp://DriverSystem@"+MasterConfConfig.host+":"+MasterConfConfig.akkaPort+"/user/driverActor";

//        ExecutorEnv.driverUrl="akka.tcp://DriverSystem@127.0.0.1:4040/user/driverActor";
//        ExecutorEnv.core=2;
        ExecutorEnv.executorUrl="akka.tcp://ExecutorSystem@"+ ExecutorEnv.host+":"+ExecutorEnv.port+"/user/executorActor";

//        ExecutorEnv.shufflePort=7737 ;


        new Thread(() -> {
            try {
                new ShuffleService(ExecutorEnv.shufflePort).start();
            } catch (InterruptedException e) {
                new RuntimeException(e);
            }
        }).start();
        ActorSystem executorSystem = ExecutorSystem.getExecutorSystem();
        ActorRef clientActorRef = executorSystem.actorOf(Props.create(ExecutorActor.class), "executorActor");
        System.out.println("ServerActor started at: " + clientActorRef.path().toString());
        ExecutorRpc.register(new ExecutorRegister(ExecutorEnv.executorUrl,ExecutorEnv.memory,ExecutorEnv.core));
    }

    public static void InitMasterCOnfConfig(){

        String systemDirectory = System.getProperty("user.dir");

        //读取bin下的文件
        File file=new File ("master.conf");
        System.out.println(file.exists());
        String masterConfPath=file.getAbsolutePath();
        // String ConfPath=systemDirectory+ File.separator+"bin";
        //String masterConfPath=ConfPath+File.separator+"master.conf";
        System.out.println("master.conf文件路径为："+masterConfPath);
        try (BufferedReader br = new BufferedReader(new FileReader(masterConfPath))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (!line.trim().startsWith("#")){
                    String[] data = line.split("\\s+");
                    MasterConfConfig.host=data[0];
                    MasterConfConfig.akkaPort=Integer.parseInt(data[1]);
                    MasterConfConfig.thriftPort=Integer.parseInt(data[2]);
                    MasterConfConfig.memory=data[3];
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        // MasterConfConfig.getInfo();
    }


    public static void InitUrlTopNConfConfig(){

//        String systemDirectory = System.getProperty("user.dir");
//
//        //读取bin下的文件
//        String ConfPath=systemDirectory+ File.separator+"bin";
//        String masterConfPath=ConfPath+File.separator+"urltopn.conf";
        //读取bin下的文件
        File file=new File ("urltopn.conf");
        System.out.println(file.exists());
        String masterConfPath=file.getAbsolutePath();
        System.out.println("urltopn.conf文件路径为："+masterConfPath);
        try (BufferedReader br = new BufferedReader(new FileReader(masterConfPath))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (!line.trim().startsWith("#")){
                    String[] data = line.split("\\s+");
                    UrltopNConfConfig.applicationId=data[0];
                    UrltopNConfConfig.inputPath=data[1];
                    UrltopNConfConfig.outputPath=data[2];
                    UrltopNConfConfig.reduceTaskNum=Integer.parseInt(data[4]);
                    UrltopNConfConfig.size=Long.parseLong(data[5]);


                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        UrltopNConfConfig.getInfo();
    }

}

