package com.allyes.hdfsutils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

public class GetObservReportJSON {

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        String hdfsBaseUrl = "hdfs://sm98:9000";
        String observReportHDFSDir = "/user/hadoop/mifc_etl/output_observer_report";
        String observReportBEDir = "C:\\Users\\zhoujie\\Desktop";
        conf.set("fs.default.name", hdfsBaseUrl);
        Path inputPath = new Path(hdfsBaseUrl + observReportHDFSDir);
        try {
            FileSystem fs = FileSystem.get(conf);
            List<FileStatus> resultList = HadoopUtils.listFiles(fs, inputPath, false);
            long maxTimestamp = 0L;
            FileStatus newestFile = null;
            for (FileStatus fileStatus : resultList) {
                System.out.println(fileStatus.getPath());
                long timeStamp = fileStatus.getModificationTime();
                System.out.println(timeStamp);
                if (timeStamp > maxTimestamp) {
                    maxTimestamp = timeStamp;
                    newestFile = fileStatus;
                }
            }
            if (null != newestFile) {
                String outputFileName = newestFile.getPath().getName();
                System.out.println("outputFileName" + outputFileName);
                List<String> obervReportList = HadoopUtils.loadLines(conf, newestFile.getPath());
                FileOutputStream out = null;
                if (null != obervReportList && obervReportList.size() == 1) {
                    // write content to local system file
                    writeString2Output(observReportBEDir, outputFileName, obervReportList, out);
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void writeString2Output(String observReportBEDir, String outputFileName,
            List<String> obervReportList, FileOutputStream out) throws IOException {
        try {
            File file = new File(observReportBEDir + File.separator + outputFileName);
            System.out.println("file path : " + file.getPath());
            if (!file.exists()) {
                file.createNewFile();
            }
            out = new FileOutputStream(file, false);
            out.write(obervReportList.get(0).getBytes("utf-8"));
        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            if (out != null) {
                out.close();
            }
        }
    }

}
