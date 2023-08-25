package com.ksc.wordcount.thrift;

import com.ksc.urltopn.thrift.UrlTopNAppRequest;
import com.ksc.urltopn.thrift.UrlTopNAppResponse;
import com.ksc.urltopn.thrift.UrlTopNResult;
import com.ksc.urltopn.thrift.UrlTopNService;
import com.ksc.wordcount.conf.MasterConfConfig;
import com.ksc.wordcount.conf.UrltopNConfConfig;

import java.io.*;
import java.util.ArrayList;

public class ThriftServiceImpl implements UrlTopNService.Iface {
    @Override
    public UrlTopNAppResponse submitApp(UrlTopNAppRequest urlTopNAppRequest) {

        writeObjectToFile(urlTopNAppRequest, "object.ser");

        Object objRead = readObjectFromFile("object.ser");
        if (objRead instanceof UrlTopNAppRequest) {
            UrlTopNAppRequest readObj = (UrlTopNAppRequest) objRead;
            System.out.println("Read object: " + readObj);
        }
        System.out.println("hello thrift");
//        MergeURLTopNDriver mergeURLTopNDriver =new MergeURLTopNDriver();
        //MergeURLTopNDriver.thriftSubmitApp(urlTopNAppRequest);


        UrlTopNAppResponse urlTopNAppResponse = new UrlTopNAppResponse();
        urlTopNAppResponse.setAppStatus(1);
        urlTopNAppResponse.setApplicationId(urlTopNAppRequest.getApplicationId());
        return urlTopNAppResponse;
    }

    @Override
    public UrlTopNAppResponse getAppStatus(String applicationId) {
        UrlTopNAppResponse urlTopNAppResponse = new UrlTopNAppResponse();
        urlTopNAppResponse.setApplicationId(applicationId);
        urlTopNAppResponse.setAppStatus(1);
        try {
            Thread.sleep(15000);
            urlTopNAppResponse.setAppStatus(2);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return urlTopNAppResponse;
    }

    @Override
    public java.util.List<UrlTopNResult> getTopNAppResult(String applicationId) {
        java.util.List<UrlTopNResult> listURL = new ArrayList<UrlTopNResult>();
        InitUrlTopNConfConfig();
        String directoryPath = UrltopNConfConfig.outputPath;
        // Get the list of files in the directory
        File directory = new File(directoryPath);
        File[] files = directory.listFiles();

        // Check if there is exactly one file in the directory
        if (files == null || files.length != 1) {
            System.out.println("Expected one file in the directory.");
            return null;
        }

        // Read the content of the single file
        File singleFile = files[0];
        try (BufferedReader reader = new BufferedReader(new FileReader(singleFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                UrlTopNResult urlTopNResult = new UrlTopNResult();
                String strs[] = line.split("\\s");
                urlTopNResult.setUrl(strs[0]);
                urlTopNResult.setCount(Integer.parseInt(strs[1]));
                listURL.add(urlTopNResult);
                System.out.println(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return listURL;
    }


    public static void writeObjectToFile(Object obj, String fileName) {
        try (FileOutputStream fileOut = new FileOutputStream(fileName);
             ObjectOutputStream objectOut = new ObjectOutputStream(fileOut)) {

            objectOut.writeObject(obj);
            System.out.println("Object written to file: " + fileName);
        } catch (IOException e) {
            e.printStackTrace();
        }
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

    public static void InitMasterCOnfConfig() {

//        String systemDirectory = System.getProperty("user.dir");
//
//        //读取bin下的文件
//        String ConfPath=systemDirectory+ File.separator+"bin";
//        String masterConfPath=ConfPath+File.separator+"master.conf";
        File file = new File("master.conf");
        System.out.println(file.exists());
        String masterConfPath = file.getAbsolutePath();
        System.out.println("master.conf文件路径为：" + masterConfPath);
        try (BufferedReader br = new BufferedReader(new FileReader(masterConfPath))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (!line.trim().startsWith("#")) {
                    String[] data = line.split("\\s+");
                    MasterConfConfig.host = data[0];
                    MasterConfConfig.akkaPort = Integer.parseInt(data[1]);
                    MasterConfConfig.thriftPort = Integer.parseInt(data[2]);
                    MasterConfConfig.memory = data[3];
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        // MasterConfConfig.getInfo();
    }

    public static void InitUrlTopNConfConfig() {

        File file = new File("urltopn.conf");
        System.out.println(file.exists());
        String masterConfPath = file.getAbsolutePath();
        System.out.println("urltopn.conf文件路径为：" + masterConfPath);
        try (BufferedReader br = new BufferedReader(new FileReader(masterConfPath))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (!line.trim().startsWith("#")) {
                    String[] data = line.split("\\s+");
                    UrltopNConfConfig.applicationId = data[0];
                    UrltopNConfConfig.inputPath = data[1];
                    UrltopNConfConfig.outputPath = data[2];
                    UrltopNConfConfig.reduceTaskNum = Integer.parseInt(data[4]);
                    UrltopNConfConfig.size = Long.parseLong(data[5]);


                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        UrltopNConfConfig.getInfo();
    }
}
