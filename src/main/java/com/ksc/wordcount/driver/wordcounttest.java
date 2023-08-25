package com.ksc.wordcount.driver;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.ksc.wordcount.datasourceapi.FileFormat;
import com.ksc.wordcount.datasourceapi.PartionFile;
import com.ksc.wordcount.datasourceapi.PartionWriter;
import com.ksc.wordcount.datasourceapi.UnsplitFileFormat;
import com.ksc.wordcount.rpc.Driver.DriverActor;
import com.ksc.wordcount.rpc.Driver.DriverSystem;
import com.ksc.wordcount.shuffle.ShuffleBlockId;
import com.ksc.wordcount.task.KeyValue;
import com.ksc.wordcount.task.map.MapFunction;
import com.ksc.wordcount.task.map.MapTaskContext;
import com.ksc.wordcount.task.reduce.ReduceFunction;
import com.ksc.wordcount.task.reduce.ReduceTaskContext;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Stream;

public class wordcounttest {

    public static void main(String[] args) {




        //System.setProperty("akka.config", "C:\\Users\\liubingxin\\Desktop\\wordcountdemo2\\bin\\master.conf");
//        DriverEnv.host="192.168.1.229";
//        DriverEnv.host="192.168.1.151";

        DriverEnv.host= "127.0.0.1";
        DriverEnv.port = 4040;
        String inputPath = "C:/tmp/output";
        String outputPath = "C:/tmp/output1";
        String applicationId = "wordcount_001";
        int reduceTaskNum = 1;

        FileFormat secondfileFormat = new UnsplitFileFormat();
        PartionFile[]  secondpartionFiles = secondfileFormat.getSplits(inputPath, 1000);

        TaskManager taskManager = DriverEnv.taskManager;

        ActorSystem executorSystem = DriverSystem.getExecutorSystem();
        ActorRef driverActorRef = executorSystem.actorOf(Props.create(DriverActor.class), "driverActor");
        System.out.println("ServerActor started at: " + driverActorRef.path().toString());


        int secondmapStageId = 2 ;
        //添加stageId和任务的映射
        taskManager.registerBlockingQueue(secondmapStageId, new LinkedBlockingQueue());
        for (PartionFile partionFile : secondpartionFiles) {
            MapFunction wordCountMapFunction = new MapFunction<String, KeyValue>() {

                @Override
                public Stream<KeyValue> map(Stream<String> stream) {
                    //todo 学生实现 定义maptask处理数据的规则
                    // stream.forEach(line->System.out.print("流式读取的数据："+line));
                    // return stream.flatMap(line->Stream.of(LogUrlParser.extractUrlsFromFile()))
                    return stream.flatMap(line -> {
                        String[] parts = line.split("\\s+");
                        String url = parts[0];
                        int count = Integer.parseInt(parts[1]);
                        KeyValue keyValue = new KeyValue(url, count);
                        return Stream.of(keyValue);
                    });

                }
            };
            MapTaskContext mapTaskContext = new MapTaskContext(applicationId, "stage_"+secondmapStageId, taskManager.generateTaskId(), partionFile.getPartionId(), partionFile,
                    secondfileFormat.createReader(), reduceTaskNum, wordCountMapFunction,true);
            taskManager.addTaskContext(secondmapStageId,mapTaskContext);
        }

        //提交stageId
        DriverEnv.taskScheduler.submitTask(secondmapStageId);
        DriverEnv.taskScheduler.waitStageFinish(secondmapStageId);



        //reduce阶段
        int reduceStageId = 3 ;
        taskManager.registerBlockingQueue(reduceStageId, new LinkedBlockingQueue());
        for(int i = 0; i < reduceTaskNum; i++){
            ShuffleBlockId[] stageShuffleIds = taskManager.getStageShuffleIdByReduceId(secondmapStageId, i);
            ReduceFunction<String, Integer, String, Integer> reduceFunction = new ReduceFunction<String, Integer, String, Integer>() {

                @Override
                public Stream<KeyValue<String, Integer>> reduce(Stream<KeyValue<String, Integer>> stream) {
                    HashMap<String, Integer> map = new HashMap<>();
                    //todo 学生实现 定义reducetask处理数据的规则
                    stream.forEach(e->{
                        String key=e.getKey();
                        Integer value=e.getValue();
                        if(map.containsKey(key)){
                            map.put(key,map.get(key)+value);
                        }else {
                            map.put(key,value);
                        }
                    });

                    // 将 map 按照键值的从高到低排序
                    Stream<KeyValue<String, Integer>> sortedStream = map.entrySet().stream()
                            .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                            .map(e -> new KeyValue<>(e.getKey(), e.getValue()));

                    return sortedStream;

                    // return map.entrySet().stream().map(e -> new KeyValue(e.getKey(), e.getValue()));
                }
            };
            PartionWriter partionWriter = secondfileFormat.createWriter(outputPath, i);
            ReduceTaskContext reduceTaskContext = new ReduceTaskContext(applicationId, "stage_" + reduceStageId, taskManager.generateTaskId(), i, stageShuffleIds, reduceFunction, partionWriter);
            taskManager.addTaskContext(reduceStageId, reduceTaskContext);
        }

        DriverEnv.taskScheduler.submitTask(reduceStageId);
        DriverEnv.taskScheduler.waitStageFinish(reduceStageId);
        System.out.println("job finished");










    }
}
