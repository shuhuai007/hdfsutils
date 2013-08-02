package com.allyes.hdfsutils;

import org.apache.commons.lang.StringUtils;
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
        if (checkHelpArg(args)) {
            printHelpInfo();
            return;
        } else {
//            String hdfsHostUrl = "hdfs://sm98:9000";
//            String observReportHDFSDir = "/user/hadoop/mifc_etl/output_observer_report";
//            String observReportBEDir = "C:\\Users\\zhoujie\\Desktop";
            String hdfsHostUrl = null;
            String observReportHDFSDir = null;
            String observReportBEDir = null;
            if (args.length > 0) {
                for (String arg : args) {
                    if (arg.startsWith("--hdfsHostUrl=")) {
                        hdfsHostUrl = getArgValue(arg);
                    } else if (arg.startsWith("--observReportHDFSDir=")) {
                        observReportHDFSDir = getArgValue(arg);
                    } else if (arg.startsWith("--observReportBEDir=")) {
                        observReportBEDir = getArgValue(arg);
                    }
                }
            }
            if(StringUtils.isBlank(hdfsHostUrl) || StringUtils.isBlank(observReportHDFSDir) || StringUtils.isBlank(observReportBEDir)){
                System.out.println("Lack parameters.");
                printHelpInfo();
                return;
            }

            Configuration conf = new Configuration();

            conf.set("fs.default.name", hdfsHostUrl);
            Path inputPath = new Path(hdfsHostUrl + observReportHDFSDir);
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

      
    }

    private static void printHelpInfo() {
        System.out.println("Usage: GetObservReportJSON <options>");
        System.out.println();
        System.out.println("Options:");
        System.out.println();
        System.out.println("--hdfsHostUrl=DIR                        hdfs host url, e.g. hdfs://sm98:9000");
        System.out.println("--observReportHDFSDir=DIR                The directory  in hdfs where the results of the observReport json locate");
        System.out.println("--observReportBEDir=DIR                  The directory  in backend machine where the observReport json will be copied to");
        System.out.println();
        System.out.println("e.g.");
        System.out.println("--hdfsHostUrl=hdfs://sm98:9000");
        System.out.println("--observReportHDFSDir=/user/hadoop/mifc_etl/output_observer_report");
        System.out.println("--observReportBEDir=C:\\Users\\zhoujie\\Desktop");
    }

    private static boolean checkHelpArg(String[] args) {
        return args.length == 1 && (args[0].equals("--help") || args[0].equals("-h") || args[0].equals("/?"));
    }

    private static String getArgValue(String arg) {
        String result = null;

        String[] tokens = arg.split("=");
        if (tokens.length > 1) {
            result = tokens[1].replace("'", "").replace("\"", "");
        }

        return result;
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
