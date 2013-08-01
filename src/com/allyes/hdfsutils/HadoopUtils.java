package com.allyes.hdfsutils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.input.LzoProtobufB64LineRecordReader;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.TypeRef;

public final class HadoopUtils {
    private HadoopUtils() {
    }

    public static final PathFilter NONE_FILE_FILTER = new PathFilter() {
        @Override
        public boolean accept(@SuppressWarnings("unused") Path path) {
            return true;
        }
    };

    public static final PathFilter HIDDEN_FILE_FILTER = new PathFilter(){
        @Override
        public boolean accept(Path p){
            String name = p.getName(); 
            return !name.startsWith("_") && !name.startsWith("."); 
        }
    };

    /**
     * get children file of dirPath.
     * for example: 
     *  Configuration conf = new Configuration();
     *  String hdfsBaseUrl = "hdfs://hdfshostname:9000";
     *  conf.set("fs.default.name",hdfsBaseUrl);
     *  Path inputPath = new Path(hdfsBaseUrl + "/user/hadoop/");
     * @param dirPath
     * @param conf
     * @return
     * @throws IOException
     */
    public static List<Path> getJobOutputFileList(Path dirPath, 
            Configuration conf) throws IOException {
        return getJobOutputFileList(dirPath, FileSystem.get(conf));
    }

    public static List<Path> getJobOutputFileList(Path outputPath, 
            FileSystem fs) throws IOException {
        List<Path> ret = new ArrayList<Path>();
        for(FileStatus e: fs.listStatus(outputPath, 
                HadoopUtils.HIDDEN_FILE_FILTER)) {
            if(!e.isDir()) {
                ret.add(e.getPath());
            }
        }

        return ret;
    }


    public static List<FileStatus> listFiles(FileSystem fs, Path path, 
            boolean recursive) throws IOException {
        return listFiles(fs, path, NONE_FILE_FILTER, recursive);
    }

    public static List<FileStatus> listFiles(FileSystem fs, Path path,
            PathFilter pathFilter, boolean recursive) throws IOException {
        List<FileStatus> files = new ArrayList<FileStatus>();
        Queue<Path> dirs = new LinkedList<Path>();
        dirs.add(path);
        while(!dirs.isEmpty()) {
            for(FileStatus f: fs.listStatus(dirs.poll(), pathFilter)) {
                if(f.isDir()) {
                  if(recursive) {
                      dirs.add(f.getPath());
                  }
                } else {
                    files.add(f);
                }
            }
        }
        
        return files;
    }

    public static interface RecursiveMoveObserver {
        void fileMoved(Path oldPath, Path newPath);
        void dirMoved(Path oldPath, Path newPath);
    }

    private static boolean recursiveMove(FileSystem fs, Path fromDir,
            Path toDir, PathFilter pathFilter, boolean topLevel,
            RecursiveMoveObserver observer) throws IOException {

        if(!fs.getFileStatus(fromDir).isDir() || 
                !fs.getFileStatus(toDir).isDir()) {
            throw new IllegalArgumentException("<from:" + fromDir + 
                    "> or <to:" + toDir + "> is not directory");
        }

        for(FileStatus f: fs.listStatus(fromDir, pathFilter)) {
            if(f.isDir()) {
                Path p = new Path(toDir, f.getPath().getName());
                if(!fs.exists(p)) {
                    if(!fs.mkdirs(p)) {
                        return false;
                    }
                }
                if(!recursiveMove(fs, f.getPath(), p, pathFilter, 
                        topLevel ? !topLevel : topLevel, observer)) {
                    return false;
                }
            } else {
                Path path = f.getPath();
                if(!fs.rename(path, toDir)) {
                    return false;
                }
                if(observer != null) {
                    Path newPath = new Path(toDir, path.getName());
                    observer.fileMoved(path, newPath);
                }
            }
        }

        if(!topLevel) {
            fs.delete(fromDir, true);
            if(observer != null) {
                observer.dirMoved(fromDir, toDir);
            }
        }

        return true;
    }

    public static boolean recursiveMove(FileSystem fs, Path fromDir, 
            Path toDir, PathFilter pathFilter, RecursiveMoveObserver observer
            ) throws IOException {
        return recursiveMove(fs, fromDir, toDir, pathFilter, true, observer);
    }

    /**
     * Move the files from fromDir to toDir recursively, the dirs and files 
     * with name starting with . or _ will be ignored 
     */
    public static boolean recursiveMove(FileSystem fs, Path fromDir,
            Path toDir) throws IOException {
        return recursiveMove(fs, fromDir, toDir, 
                HadoopUtils.HIDDEN_FILE_FILTER, null);
    }
 
    public static List<String> loadLines(Configuration conf, Path inputPath,
            PathFilter pathFilter) throws IOException {
        List<String> ret = new ArrayList<String>();

        FileSystem fs = FileSystem.get(conf);

        CompressionCodecFactory cf = new CompressionCodecFactory(conf);

        for(FileStatus e: fs.listStatus(inputPath, pathFilter)) {
            BufferedReader reader = null;
            try {
                InputStream i = fs.open(e.getPath());
                CompressionCodec codec = cf.getCodec(e.getPath());
                if (codec != null) {
                    i = codec.createInputStream(i);
                }
                reader = new BufferedReader(new InputStreamReader(i));
                String line = null;
                while(null != (line=reader.readLine())) {
                    ret.add(line);
                }
            } finally {
                IOUtils.closeStream(reader);
            }
        }

        return ret;
    }

    public static <T extends Message> LzoProtobufB64LineRecordReader<T> 
        getMessageRecordReader(Configuration conf, 
                Class<? extends Message> protoClass, Path file
                ) throws IOException, InterruptedException {
        InputSplit split = new FileSplit(file, 0, 
                FileSystem.get(conf).getFileStatus(file).getLen(),
                (String[])null);

        TypeRef<T> typeRef = new TypeRef<T>(protoClass) {};

        LzoProtobufB64LineRecordReader<T> reader = 
            new LzoProtobufB64LineRecordReader<T>(typeRef);

        reader.initialize(split, new TaskAttemptContext(conf,
                new TaskAttemptID()));

        return reader;
    }

    @SuppressWarnings("unchecked")
    public static <T extends Message> List<T> loadMessages(Configuration conf,
            Class<? extends Message> protoClass, Path inputPath
            ) throws IOException, InterruptedException {
        List<T> ret = new ArrayList<T>();

        TypeRef<T> typeRef = new TypeRef<T>(protoClass) {};
        FileSystem fs = FileSystem.get(conf);
        for(FileStatus e: fs.listStatus(inputPath,
                HIDDEN_FILE_FILTER)) {
            LzoProtobufB64LineRecordReader<T> reader = 
                getMessageRecordReader(conf, protoClass, e.getPath()); 
            while(reader.nextKeyValue()) {
                ret.add((T) Protobufs.getMessageBuilder(typeRef.getRawClass()).
                        mergeFrom(reader.getCurrentValue().get()).build());
            }
        }

        return ret;
    }

    public static List<String> loadLines(Configuration conf, Path inputPath
            ) throws IOException {
        List<String> ret = new ArrayList<String>();

        FileSystem fs = FileSystem.get(conf);
        
        CompressionCodecFactory cf = new CompressionCodecFactory(conf);


        for(FileStatus e: fs.listStatus(inputPath)) {
            InputStream i = fs.open(e.getPath());
            CompressionCodec codec = cf.getCodec(e.getPath());
            if (codec != null) {
                i = codec.createInputStream(i);
            }

            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(i));
            String line = null;
            while(null != (line=reader.readLine())) {
                ret.add(line);
            }
        }

        return ret;
    }
    
    
    /**
     * 将outContent的信息写入hdfs中
     * 
     * @param fs
     * @param outPath
     * @param outContent
     * @throws IOException
     */
    public static void writeContentToHDFS(FileSystem fs, Path outPath,
            String outContent) throws IOException {
        FSDataOutputStream outputStream = null;
        try {
            outputStream = fs.create(outPath);
            outputStream.write(outContent.getBytes(), 0, outContent.length());
        } finally {
            outputStream.close();
        }
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        String hdfsBaseUrl = "hdfs://sm98:9000";
        conf.set("fs.default.name",hdfsBaseUrl);
        Path inputPath = new Path(hdfsBaseUrl + "/user/hadoop/");
        try {
            List<Path> resultList = getJobOutputFileList(inputPath, conf);
            for(Path filePath : resultList){
                System.out.println(filePath.toString());
                
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
