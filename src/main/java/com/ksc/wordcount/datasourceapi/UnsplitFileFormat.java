package com.ksc.wordcount.datasourceapi;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class UnsplitFileFormat implements FileFormat {

        @Override
        public boolean isSplitable(String filePath) {
            return true;
        }


        @Override
        public PartionFile[] getSplits(String filePath, long size) {
            File parentFile=new File(filePath);

            if(parentFile.isFile()){
                return new PartionFile[]{new PartionFile(0,new FileSplit[]{new FileSplit(filePath,0, parentFile.length())})};
            }
            List<PartionFile> partiongFileList=new ArrayList<>();
            //todo 学生实现 driver端切分split的逻辑

            File[] files =parentFile.listFiles();
            int partionId=0;
            for(File file:files){
                FileSplit[] fileSplit={new FileSplit(file.getAbsolutePath(),0,file.length())};

//                FileSplit[] fileSplit={new FileSplit(file.getAbsolutePath(),0,file.length()/2-1),new FileSplit(file.getAbsolutePath(),file.length()/2,file.length())};
                partiongFileList.add(new PartionFile(partionId,fileSplit));
                partionId++;
            }
            return partiongFileList.toArray(new PartionFile[partiongFileList.size()]);
        }

    @Override
    public PartionReader createReader() {
        return new TextPartionReader();
    }



    @Override
    public PartionWriter createWriter(String destPath, int partionId) {
        return new TextPartionWriter(destPath, partionId);
    }


}
