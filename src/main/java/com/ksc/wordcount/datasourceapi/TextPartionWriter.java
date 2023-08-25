package com.ksc.wordcount.datasourceapi;

import com.ksc.wordcount.task.KeyValue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

public class TextPartionWriter implements PartionWriter<KeyValue>, Serializable {

    private String destDest;
    private int partionId;

    public TextPartionWriter(String destDest,int partionId){
         this.destDest = destDest;
         this.partionId = partionId;
    }

    //把partionId 前面补0，补成length位
    public String padLeft(int partionId,int length){
        String partionIdStr = String.valueOf(partionId);
        int len = partionIdStr.length();
        if(len<length){
            for(int i=0;i<length-len;i++){
                partionIdStr = "0"+partionIdStr;
            }
        }
        return partionIdStr;
    }

    //todo 学生实现 将reducetask的计算结果写入结果文件中
    @Override
    public void write(Stream<KeyValue> stream,String applicationId) throws IOException {

//        File file=new File(destDest+File.separator+applicationId+".txt");

        File file=new File(destDest+File.separator+"part_"+padLeft(partionId,3)+".txt");
        if(!file.getParentFile().exists()){file.getParentFile().mkdirs();}
        try(FileOutputStream fos=new FileOutputStream(file)){
            stream.forEach(keyValue -> {
                try{

                    fos.write((keyValue.getKey()+"\t"+keyValue.getValue()+"\n").getBytes(StandardCharsets.UTF_8));
                }catch (Exception e){
                    e.printStackTrace();
                }
            });
        }

    }

}
