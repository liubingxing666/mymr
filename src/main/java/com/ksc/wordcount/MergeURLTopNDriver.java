package com.ksc.wordcount;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.ksc.urltopn.thrift.UrlTopNAppRequest;
import com.ksc.wordcount.conf.MasterConfConfig;
import com.ksc.wordcount.conf.UrltopNConfConfig;
import com.ksc.wordcount.datasourceapi.FileFormat;
import com.ksc.wordcount.datasourceapi.PartionFile;
import com.ksc.wordcount.datasourceapi.PartionWriter;
import com.ksc.wordcount.datasourceapi.UnsplitFileFormat;
import com.ksc.wordcount.driver.DriverEnv;
import com.ksc.wordcount.driver.TaskManager;
import com.ksc.wordcount.rpc.Driver.DriverActor;
import com.ksc.wordcount.rpc.Driver.DriverSystem;
import com.ksc.wordcount.rpc.Executor.ExecutorActor;
import com.ksc.wordcount.rpc.Executor.ExecutorRpc;
import com.ksc.wordcount.rpc.Executor.ExecutorSystem;
import com.ksc.wordcount.rpc.ExecutorRegister;
import com.ksc.wordcount.shuffle.ShuffleBlockId;
import com.ksc.wordcount.shuffle.nettyimpl.server.ShuffleService;
import com.ksc.wordcount.task.KeyValue;
import com.ksc.wordcount.task.map.MapFunction;
import com.ksc.wordcount.task.map.MapTaskContext;
import com.ksc.wordcount.task.reduce.ReduceFunction;
import com.ksc.wordcount.task.reduce.ReduceTaskContext;
import com.ksc.wordcount.worker.ExecutorEnv;

import java.io.*;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MergeURLTopNDriver implements Serializable {
    public static ActorSystem executorSystem = null;
    public static ActorRef driverActorRef = null;
    public static void main(String[] args) {
        InitMasterCOnfConfig();
        InitUrlTopNConfConfig();


        DriverEnv.host = MasterConfConfig.host;
        DriverEnv.port = MasterConfConfig.akkaPort;
        String inputPath =UrltopNConfConfig.inputPath;
        String outputPath = System.getProperty("java.io.tmpdir")+"/middleOutput";
        File file =new File(outputPath);
        if(!file.exists())
            file.mkdirs();
        String applicationId=UrltopNConfConfig.applicationId;
        int reduceTaskNum=UrltopNConfConfig.reduceTaskNum;
//        DriverEnv.host = "127.0.0.1;
//        DriverEnv.port = "4040";
//        String inputPath ="C:/tmp/input";
//        String outputPath = "C:/tmp/output";
//        String applicationId = "wordcount_001";
//        int reduceTaskNum = 2;

        FileFormat fileFormat = new UnsplitFileFormat();
        PartionFile[] partionFiles = fileFormat.getSplits(inputPath, UrltopNConfConfig.size);

        TaskManager taskManager = DriverEnv.taskManager;

        executorSystem = DriverSystem.getExecutorSystem();
        driverActorRef = executorSystem.actorOf(Props.create(DriverActor.class), "driverActor");
        System.out.println("ServerActor started at: " + driverActorRef.path().toString());


        int mapStageId = 0;
        //添加stageId和任务的映射
        taskManager.registerBlockingQueue(mapStageId, new LinkedBlockingQueue());
        for (PartionFile partionFile : partionFiles) {
            MapFunction wordCountMapFunction = new MapFunction<String, KeyValue>() {

                @Override
                public Stream<KeyValue> map(Stream<String> stream) {
                    //todo 学生实现 定义maptask处理数据的规则
                    return stream.flatMap(line -> Stream.of(line.split("\\s+"))).map(word -> new KeyValue(word, 1));
                }
            };
            MapTaskContext mapTaskContext = new MapTaskContext(applicationId, "stage_" + mapStageId, taskManager.generateTaskId(), partionFile.getPartionId(), partionFile,
                    fileFormat.createReader(), reduceTaskNum, wordCountMapFunction, false);
            taskManager.addTaskContext(mapStageId, mapTaskContext);
        }

        //提交stageId
        DriverEnv.taskScheduler.submitTask(mapStageId);
        DriverEnv.taskScheduler.waitStageFinish(mapStageId);


        //reduce阶段
        int reduceStageId = 1;
        taskManager.registerBlockingQueue(reduceStageId, new LinkedBlockingQueue());
        for (int i = 0; i < reduceTaskNum; i++) {
            ShuffleBlockId[] stageShuffleIds = taskManager.getStageShuffleIdByReduceId(mapStageId, i);
            ReduceFunction<String, Integer, String, Integer> reduceFunction = new ReduceFunction<String, Integer, String, Integer>() {

                @Override
                public Stream<KeyValue<String, Integer>> reduce(Stream<KeyValue<String, Integer>> stream) {
                    HashMap<String, Integer> map = new HashMap<>();
                    //todo 学生实现 定义reducetask处理数据的规则
                    stream.forEach(e -> {
                        String key = e.getKey();
                        Integer value = e.getValue();
                        if (map.containsKey(key)) {
                            map.put(key, map.get(key) + value);
                        } else {
                            map.put(key, value);
                        }
                    });

                    // 将 map 按照键值的从高到低排序
                    Stream<KeyValue<String, Integer>> sortedStream = map.entrySet().stream()
                            .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                            .map(e -> new KeyValue<>(e.getKey(), e.getValue()));

                    return sortedStream;

                }
            };
            PartionWriter partionWriter = fileFormat.createWriter(outputPath, i);
            ReduceTaskContext reduceTaskContext = new ReduceTaskContext(applicationId, "stage_" + reduceStageId, taskManager.generateTaskId(), i, stageShuffleIds, reduceFunction, partionWriter);
            taskManager.addTaskContext(reduceStageId, reduceTaskContext);
        }

        DriverEnv.taskScheduler.submitTask(reduceStageId);
        DriverEnv.taskScheduler.waitStageFinish(reduceStageId);
        System.out.println("job1 finished");


        DriverEnv.host = MasterConfConfig.host;
        DriverEnv.port = MasterConfConfig.akkaPort;
        String secondinputPath =System.getProperty("java.io.tmpdir")+"/middleOutput";;

        //TODO
        String secondoutputPath = UrltopNConfConfig.outputPath;

        File file1 =new File(secondoutputPath);
        if(!file1.exists())
            file1.mkdirs();
        int secondreduceTaskNum = 1;
//
        FileFormat secondfileFormat = new UnsplitFileFormat();
        PartionFile[] secondpartionFiles = secondfileFormat.getSplits(secondinputPath, 1000);
//       String applicationId = "wordcount_001";
//        TaskManager taskManager = DriverEnv.taskManager;
////
//        ActorSystem executorSystem = DriverSystem.getExecutorSystem();
//        ActorRef driverActorRef = executorSystem.actorOf(Props.create(DriverActor.class), "driverActor");
        System.out.println("ServerActor started at: " + driverActorRef.path().toString());


        int secondmapStageId = 2;
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
            MapTaskContext mapTaskContext = new MapTaskContext(applicationId, "stage_" + secondmapStageId, taskManager.generateTaskId(), partionFile.getPartionId(), partionFile,
                    secondfileFormat.createReader(), secondreduceTaskNum, wordCountMapFunction, true);
            taskManager.addTaskContext(secondmapStageId, mapTaskContext);
        }

        //提交stageId
        DriverEnv.taskScheduler.submitTask(secondmapStageId);
        DriverEnv.taskScheduler.waitStageFinish(secondmapStageId);


        //reduce阶段
        int secondreduceStageId = 3;
        taskManager.registerBlockingQueue(secondreduceStageId, new LinkedBlockingQueue());
        for (int i = 0; i < secondreduceTaskNum; i++) {
            ShuffleBlockId[] stageShuffleIds = taskManager.getStageShuffleIdByReduceId(secondmapStageId, i);
            ReduceFunction<String, Integer, String, Integer> reduceFunction = new ReduceFunction<String, Integer, String, Integer>() {

                @Override
                public Stream<KeyValue<String, Integer>> reduce(Stream<KeyValue<String, Integer>> stream) {

                    Map<String, Integer> map = stream.collect(Collectors.toMap(
                            KeyValue::getKey,
                            KeyValue::getValue,
                            Integer::sum
                    ));

                    // Sort the map by values in descending order
                    List<Map.Entry<String, Integer>> sortedList = map.entrySet().stream()
                            .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                            .collect(Collectors.toList());

                    // Select the top 3 unique values
                    Set<Integer> topValues = new HashSet<>();
                    List<Map.Entry<String, Integer>> topList = new ArrayList<>();
                    for (Map.Entry<String, Integer> entry : sortedList) {
                        if (topValues.size() >= 3) {
                            break;
                        }
                        if (topValues.add(entry.getValue())) {
                            topList.add(entry);
                        }
                    }

                    // Collect entries containing the top values
                    List<KeyValue<String, Integer>> result = map.entrySet().stream()
                            .filter(entry -> topValues.contains(entry.getValue()))
                            .map(entry -> new KeyValue<>(entry.getKey(), entry.getValue()))
                            .collect(Collectors.toList());

                    // Sort the result by values in descending order
                    result.sort(Collections.reverseOrder(Comparator.comparing(KeyValue::getValue)));

                    return result.stream();

                    // return map.entrySet().stream().map(e -> new KeyValue(e.getKey(), e.getValue()));
                }
            };


            PartionWriter partionWriter = secondfileFormat.createWriter(secondoutputPath, i);
            ReduceTaskContext reduceTaskContext = new ReduceTaskContext(applicationId, "stage_" + secondreduceStageId, taskManager.generateTaskId(), i, stageShuffleIds, reduceFunction, partionWriter);
            taskManager.addTaskContext(secondreduceStageId, reduceTaskContext);
        }

        DriverEnv.taskScheduler.submitTask(secondreduceStageId);
        DriverEnv.taskScheduler.waitStageFinish(secondreduceStageId);
        System.out.println("job2 finished");

        while (true) {

            if (new File("object.ser").exists()) {
                Object objRead = readObjectFromFile("object.ser");
                if (objRead instanceof UrlTopNAppRequest) {
                    UrlTopNAppRequest readObj = (UrlTopNAppRequest) objRead;
                    if (objRead != null) {
                        thriftSubmitApp(readObj);
                        System.out.println("Read object: " + readObj);
                        new File("object.ser").delete();

                    }
                }
            }
        }

    }

    public static void thriftSubmitApp(UrlTopNAppRequest urlTopNAppRequest) {

        System.out.println("342423423423");
//        DriverEnv.host = "127.0.0.1";
//        DriverEnv.port = 4040;
//        String inputPath = "C:/tmp/input";
//        String outputPath = "C:/tmp/output";
//        String applicationId = "wordcount_001";
//        int reduceTaskNum = 2;
        DriverEnv.host=MasterConfConfig.host;
        DriverEnv.port=MasterConfConfig.akkaPort;
        String inputPath=urlTopNAppRequest.inputPath;
        String outputPath = System.getProperty("java.io.tmpdir")+"/middleSecondOutput";
        String applicationId=urlTopNAppRequest.getApplicationId();
        int reduceTaskNum=urlTopNAppRequest.getNumReduceTasks();

        FileFormat fileFormat = new UnsplitFileFormat();
        PartionFile[] partionFiles = fileFormat.getSplits(inputPath, urlTopNAppRequest.getSplitSize());

        TaskManager taskManager = DriverEnv.taskManager;

        System.out.println("ServerActor started at: " + driverActorRef.path().toString());


        int mapStageId = 4;
        //添加stageId和任务的映射
        taskManager.registerBlockingQueue(mapStageId, new LinkedBlockingQueue());
        for (PartionFile partionFile : partionFiles) {
            MapFunction wordCountMapFunction = new MapFunction<String, KeyValue>() {

                @Override
                public Stream<KeyValue> map(Stream<String> stream) {
                    //todo 学生实现 定义maptask处理数据的规则
                    // stream.forEach(line->System.out.print("流式读取的数据："+line));
                    // return stream.flatMap(line->Stream.of(LogUrlParser.extractUrlsFromFile()))
                    return stream.flatMap(line -> Stream.of(line.split("\\s+"))).map(word -> new KeyValue(word, 1));

                }
            };
            MapTaskContext mapTaskContext = new MapTaskContext(applicationId, "stage_" + mapStageId, taskManager.generateTaskId(), partionFile.getPartionId(), partionFile,
                    fileFormat.createReader(), reduceTaskNum, wordCountMapFunction, false);
            taskManager.addTaskContext(mapStageId, mapTaskContext);
        }


        // executor();
        //提交stageId
        DriverEnv.taskScheduler.submitTask(mapStageId);

        DriverEnv.taskScheduler.waitStageFinish(mapStageId);


        //reduce阶段
        int reduceStageId = 5;
        taskManager.registerBlockingQueue(reduceStageId, new LinkedBlockingQueue());
        for (int i = 0; i < reduceTaskNum; i++) {
            ShuffleBlockId[] stageShuffleIds = taskManager.getStageShuffleIdByReduceId(mapStageId, i);
            ReduceFunction<String, Integer, String, Integer> reduceFunction = new ReduceFunction<String, Integer, String, Integer>() {

                @Override
                public Stream<KeyValue<String, Integer>> reduce(Stream<KeyValue<String, Integer>> stream) {
                    HashMap<String, Integer> map = new HashMap<>();
                    //todo 学生实现 定义reducetask处理数据的规则
                    stream.forEach(e -> {
                        String key = e.getKey();
                        Integer value = e.getValue();
                        if (map.containsKey(key)) {
                            map.put(key, map.get(key) + value);
                        } else {
                            map.put(key, value);
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
        System.out.println("job3 finished");


//        DriverEnv.host = "127.0.0.1";
//        DriverEnv.port = 4040;
//        String secondinputPath = "C:/tmp/output";
//        String secondoutputPath = "C:/tmp/output1";
//        int secondreduceTaskNum = 1;
        DriverEnv.host = MasterConfConfig.host;
        DriverEnv.port = MasterConfConfig.akkaPort;
        String secondinputPath = System.getProperty("java.io.tmpdir")+"/middleSecondOutput";
        String secondoutputPath = urlTopNAppRequest.ouputPath;
        int secondreduceTaskNum = 1;
//
        FileFormat secondfileFormat = new UnsplitFileFormat();
        PartionFile[] secondpartionFiles = secondfileFormat.getSplits(secondinputPath, 1000);
//       String applicationId = "wordcount_001";
//        TaskManager taskManager = DriverEnv.taskManager;
////
//        ActorSystem executorSystem = DriverSystem.getExecutorSystem();
//        ActorRef driverActorRef = executorSystem.actorOf(Props.create(DriverActor.class), "driverActor");
        System.out.println("ServerActor started at: " + driverActorRef.path().toString());


        int secondmapStageId = 6;
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
            MapTaskContext mapTaskContext = new MapTaskContext(applicationId, "stage_" + secondmapStageId, taskManager.generateTaskId(), partionFile.getPartionId(), partionFile,
                    secondfileFormat.createReader(), secondreduceTaskNum, wordCountMapFunction, true);
            taskManager.addTaskContext(secondmapStageId, mapTaskContext);
        }

        //提交stageId
        DriverEnv.taskScheduler.submitTask(secondmapStageId);
        DriverEnv.taskScheduler.waitStageFinish(secondmapStageId);


        //reduce阶段
        int secondreduceStageId = 7;
        taskManager.registerBlockingQueue(secondreduceStageId, new LinkedBlockingQueue());
        for (int i = 0; i < secondreduceTaskNum; i++) {
            ShuffleBlockId[] stageShuffleIds = taskManager.getStageShuffleIdByReduceId(secondmapStageId, i);
            ReduceFunction<String, Integer, String, Integer> reduceFunction = new ReduceFunction<String, Integer, String, Integer>() {

                @Override
                public Stream<KeyValue<String, Integer>> reduce(Stream<KeyValue<String, Integer>> stream) {
                    HashMap<String, Integer> map = new HashMap<>();
                    //todo 学生实现 定义reducetask处理数据的规则
                    stream.forEach(e -> {
                        String key = e.getKey();
                        Integer value = e.getValue();
                        if (map.containsKey(key)) {
                            map.put(key, map.get(key) + value);
                        } else {
                            map.put(key, value);
                        }
                    });

                    // 将 map 按照键值的从高到低排序
                    Stream<KeyValue<String, Integer>> sortedStream = map.entrySet().stream()
                            .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                            .map(e -> new KeyValue<>(e.getKey(), e.getValue()));

                    return sortedStream;
//                    Map<String, Integer> map = stream.collect(Collectors.toMap(
//                            KeyValue::getKey,
//                            KeyValue::getValue,
//                            Integer::sum
//                    ));
//
//                    // Sort the map by values in descending order
//                    List<Map.Entry<String, Integer>> sortedList = map.entrySet().stream()
//                            .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
//                            .collect(Collectors.toList());
//
//                    // Select the top 3 unique values
//                    Set<Integer> topValues = new HashSet<>();
//                    List<Map.Entry<String, Integer>> topList = new ArrayList<>();
//                    for (Map.Entry<String, Integer> entry : sortedList) {
//                        if (topValues.size() >= 3) {
//                            break;
//                        }
//                        if (topValues.add(entry.getValue())) {
//                            topList.add(entry);
//                        }
//                    }
//
//                    // Collect entries containing the top values
//                    List<KeyValue<String, Integer>> result = map.entrySet().stream()
//                            .filter(entry -> topValues.contains(entry.getValue()))
//                            .map(entry -> new KeyValue<>(entry.getKey(), entry.getValue()))
//                            .collect(Collectors.toList());
//
//                    // Sort the result by values in descending order
//                    result.sort(Collections.reverseOrder(Comparator.comparing(KeyValue::getValue)));
//
//                    return result.stream();

                    // return map.entrySet().stream().map(e -> new KeyValue(e.getKey(), e.getValue()));
                }
            };
            PartionWriter partionWriter = secondfileFormat.createWriter(secondoutputPath, i);
            ReduceTaskContext reduceTaskContext = new ReduceTaskContext(applicationId, "stage_" + secondreduceStageId, taskManager.generateTaskId(), i, stageShuffleIds, reduceFunction, partionWriter);
            taskManager.addTaskContext(secondreduceStageId, reduceTaskContext);
        }

        DriverEnv.taskScheduler.submitTask(secondreduceStageId);
        DriverEnv.taskScheduler.waitStageFinish(secondreduceStageId);
        System.out.println("job4 finished");


    }

    public static void executor() {
        ExecutorEnv.host = "127.0.0.1";

        ExecutorEnv.port = 15050;
        ExecutorEnv.memory = "512m";
//        ExecutorEnv.driverUrl="akka.tcp://DriverSystem@192.168.1.151:4040/user/driverActor";

        ExecutorEnv.driverUrl = "akka.tcp://DriverSystem@127.0.0.1:4040/user/driverActor";
        ExecutorEnv.core = 2;
        ExecutorEnv.executorUrl = "akka.tcp://ExecutorSystem@" + ExecutorEnv.host + ":" + ExecutorEnv.port + "/user/executorActor";
        ExecutorEnv.shufflePort = 7337;
        new Thread(() -> {
            try {
                new ShuffleService(ExecutorEnv.shufflePort).start();
            } catch (InterruptedException e) {
                new RuntimeException(e);
            }
        }).start();
        ActorSystem executorSystem1 = ExecutorSystem.getExecutorSystem();
        ActorRef clientActorRef = executorSystem1.actorOf(Props.create(ExecutorActor.class), "executorActor");
        System.out.println("ServerActor started at: " + clientActorRef.path().toString());
        ExecutorRpc.register(new ExecutorRegister(ExecutorEnv.executorUrl, ExecutorEnv.memory, ExecutorEnv.core));
    }

    public static Object readObjectFromFile(String fileName) {
        try (FileInputStream fileIn = new FileInputStream(fileName);
             ObjectInputStream objectIn = new ObjectInputStream(fileIn)) {

            Object obj = objectIn.readObject();
            System.out.println("Object read from file: " + fileName);
            return obj;
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void InitMasterCOnfConfig(){

        //String systemDirectory = System.getProperty("user.dir");

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
