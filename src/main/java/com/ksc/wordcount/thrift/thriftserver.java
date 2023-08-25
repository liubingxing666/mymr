package com.ksc.wordcount.thrift;

import com.ksc.urltopn.thrift.UrlTopNService;
import com.ksc.wordcount.conf.MasterConfConfig;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class thriftserver {

    public static void main(String[] args) {
        InitMasterCOnfConfig();
        try {
            // 创建Service的处理器，并关联到ServiceImpl的实现。
            UrlTopNService.Processor<ThriftServiceImpl> processor = new UrlTopNService.Processor<>(new ThriftServiceImpl());
            // 使用TServerSocket进行TCP传输，设置服务端口为9091
            TServerSocket serverSocket = new TServerSocket(MasterConfConfig.thriftPort);
            // 使用特定的序列化协议
            TProtocolFactory protocolFactory = new TBinaryProtocol.Factory();
            // 创建并启动Thrift服务器
            TServer server = new TSimpleServer(new TServer.Args(serverSocket).processor(processor).protocolFactory(protocolFactory));
            System.out.println("Server started on port "+MasterConfConfig.thriftPort+"...");
            server.serve();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void InitMasterCOnfConfig(){

//        String systemDirectory = System.getProperty("user.dir");
//
//        //读取bin下的文件
//        String ConfPath=systemDirectory+ File.separator+"bin";
//        String masterConfPath=ConfPath+File.separator+"master.conf";
        File file=new File ("master.conf");
        System.out.println(file.exists());
        String masterConfPath=file.getAbsolutePath();
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
}
