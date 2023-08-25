package com.ksc.wordcount.datasourceapi;

import com.ksc.wordcount.LogUrlParser;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class TextPartionReader implements PartionReader<String>, Serializable {


@Override
    public Stream<String> toStream(PartionFile partionFile) throws IOException {
        Stream<String> allStream = Stream.empty();
        int i=0;
        //todo 学生实现 maptask读取原始数据文件的内容
        for (FileSplit fileSplit : partionFile.getFileSplits()) {

            //1.先对一行的url进行处理
           // List<String> words = Arrays.asList("Hello", "World");
          //  Stream<String> linesStream = Files.lines(Paths.get(fileSplit.getFileName()));
           // linesStream.forEach(line -> System.out.println("流式读取的数据" + line));

//            List<String> list= LogUrlParser.extractUrlsFromFile(fileSplit.getFileName());
//            Stream<String> linesStream=list.stream();;
//            allStream = Stream.concat(allStream, linesStream);
////
//            String filePath = fileSplit.getFileName();  // Replace with the actual file path
//            long start = fileSplit.getStart();  // Start position
//            long length = fileSplit.getLength();  // Length to read
//
//
//            BufferedReader reader = Files.newBufferedReader(Paths.get(filePath), StandardCharsets.UTF_8);
//
//                // Move to the start position
//                reader.skip(start);
//                System.out.println("i:"+i);i++;
//                System.out.println("+++++start++++"+start+" "+length );
//                Stream<String> linesStream = reader.lines().limit(length)
//                        .flatMap(line -> LogUrlParser.extractUrlsFromLine(line).stream());
//                allStream = Stream.concat(allStream, linesStream);






//
            Stream<String> linesStream=Files.lines(Paths.get(fileSplit.getFileName())).flatMap(line -> LogUrlParser.extractUrlsFromLine(line).stream());
            allStream = Stream.concat(allStream, linesStream);
                     // 将每行数据提取URL并转换为流
        }
       // System.out.println(allStream.count());
//        allStream.forEach(line->System.out.print("流式读取的数据："+line));
       // System.out.println("返回数据"+(i++)+"次");
        return allStream;
    }
@Override
    public Stream<String> secondtoStream(PartionFile partionFile) throws IOException {
        Stream<String> allStream = Stream.empty();
        int i=0;

        for (FileSplit fileSplit : partionFile.getFileSplits()) {

//            String filePath = fileSplit.getFileName();  // Replace with the actual file path
//            long start = fileSplit.getStart();  // Start position
//            long length = fileSplit.getLength();  // Length to read
//            BufferedReader reader = Files.newBufferedReader(Paths.get(filePath), StandardCharsets.UTF_8);
//
//            // Move to the start position
//            reader.skip(start);
//            System.out.println("i:"+i);i++;
//            System.out.println("+++++start++++"+start+" "+length );
//            Stream<String> linesStream = reader.lines().limit(length);
//            allStream = Stream.concat(allStream, linesStream);
//            continue;


            Stream<String> linesStream=Files.lines(Paths.get(fileSplit.getFileName()));
            allStream = Stream.concat(allStream, linesStream);
            // 将每行数据提取URL并转换为流
        }
        // System.out.println(allStream.count());
//        allStream.forEach(line->System.out.print("流式读取的数据："+line));
        // System.out.println("返回数据"+(i++)+"次");
        return allStream;
    }


}
