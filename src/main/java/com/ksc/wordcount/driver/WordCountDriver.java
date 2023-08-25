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

public class WordCountDriver {

    public static void main(String[] args) {


        DriverEnv.host= "127.0.0.1";
        DriverEnv.port = 4040;
        String inputPath = "C:/tmp/input";
        String outputPath = "C:/tmp/output";
        String applicationId = "wordcount_001";
        int reduceTaskNum = 2;

        FileFormat fileFormat = new UnsplitFileFormat();
        PartionFile[]  partionFiles = fileFormat.getSplits(inputPath, 1000);

        TaskManager taskManager = DriverEnv.taskManager;

        ActorSystem executorSystem = DriverSystem.getExecutorSystem();
        ActorRef driverActorRef = executorSystem.actorOf(Props.create(DriverActor.class), "driverActor");
        System.out.println("ServerActor started at: " + driverActorRef.path().toString());
//
//        DriverEnv.host= "127.0.0.1";
//        DriverEnv.port = 4040;
//        inputPath = "C:/tmp/input";
//         outputPath = "C:/tmp/output";
//         applicationId = "wordcount_001";
//         reduceTaskNum = 2;
//
//         fileFormat = new UnsplitFileFormat();
//          partionFiles = fileFormat.getSplits(inputPath, 1000);
//
//         taskManager = DriverEnv.taskManager;
//
//         executorSystem = DriverSystem.getExecutorSystem();
//         driverActorRef = executorSystem.actorOf(Props.create(DriverActor.class), "driverActor");


        System.out.println("ServerActor started at: " + driverActorRef.path().toString());


        int mapStageId = 0 ;
        //添加stageId和任务的映射
        taskManager.registerBlockingQueue(mapStageId, new LinkedBlockingQueue());
        for (PartionFile partionFile : partionFiles) {
            MapFunction wordCountMapFunction = new MapFunction<String, KeyValue>() {

                @Override
                public Stream<KeyValue> map(Stream<String> stream) {
                    //todo 学生实现 定义maptask处理数据的规则
                    // stream.forEach(line->System.out.print("流式读取的数据："+line));
                    // return stream.flatMap(line->Stream.of(LogUrlParser.extractUrlsFromFile()))
                    return stream.flatMap(line-> Stream.of(line.split("\\s+"))).map(word->new KeyValue(word,1));

                }
            };
            MapTaskContext mapTaskContext = new MapTaskContext(applicationId, "stage_"+mapStageId, taskManager.generateTaskId(), partionFile.getPartionId(), partionFile,
                    fileFormat.createReader(), reduceTaskNum, wordCountMapFunction,false);
            taskManager.addTaskContext(mapStageId,mapTaskContext);
        }

        //提交stageId
        DriverEnv.taskScheduler.submitTask(mapStageId);
        DriverEnv.taskScheduler.waitStageFinish(mapStageId);



        //reduce阶段
        int reduceStageId = 1 ;
        taskManager.registerBlockingQueue(reduceStageId, new LinkedBlockingQueue());
        for(int i = 0; i < reduceTaskNum; i++){
            ShuffleBlockId[] stageShuffleIds = taskManager.getStageShuffleIdByReduceId(mapStageId, i);
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
            PartionWriter partionWriter = fileFormat.createWriter(outputPath, i);
            ReduceTaskContext reduceTaskContext = new ReduceTaskContext(applicationId, "stage_" + reduceStageId, taskManager.generateTaskId(), i, stageShuffleIds, reduceFunction, partionWriter);
            taskManager.addTaskContext(reduceStageId, reduceTaskContext);
        }

        DriverEnv.taskScheduler.submitTask(reduceStageId);
        DriverEnv.taskScheduler.waitStageFinish(reduceStageId);
        System.out.println("job finished");






    }
}
