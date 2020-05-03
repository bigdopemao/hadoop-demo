package com.mao.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * http://192.168.2.2:50070/explorer.html
 * http://192.168.2.2:60010/
 * @author bigdope
 * @create 2018-06-28
 **/
public class HdfsUtils {

    private final static String BASE_URL = "hdfs://192.168.2.2:9000";

    private final static String BASE_URL_UP = "/user/root/input";

    private final static String USER_SIGNAL = "root";

    public static FileSystem getFileSystem() {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.2.2:9000");
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(new URI(BASE_URL), conf, USER_SIGNAL);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return fileSystem;
    }

    public static void uploadFile(String fromFilePath, String putFilePath) {
        FileSystem fileSystem = getFileSystem();

        Path writePath = new Path(putFilePath);

        try {
            fileSystem.copyFromLocalFile(new Path(fromFilePath), writePath);
        } catch (IOException e) {
            e.printStackTrace();
        }

//        FSDataOutputStream outputStream = null;
//        try {
//            outputStream = fileSystem.create(writePath);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        FileInputStream inputStream = null;
//        try {
//            inputStream = new FileInputStream(new File(fromFilePath));
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        }
//
//        try {
//            IOUtils.copyBytes(inputStream, outputStream, 4096, false);
//        } catch (IOException e) {
//            e.printStackTrace();
//        } finally {
//            IOUtils.closeStream(inputStream);
//            IOUtils.closeStream(outputStream);
//        }

    }

    public static void read(String fileName, String fileStorgePath) {
        FileSystem fileSystem = getFileSystem();

        Path readPath = new Path(fileName);

        FSDataInputStream inputStream = null;
        try {
            ContentSummary contentSummary = fileSystem.getContentSummary(readPath);
            System.out.println(contentSummary);
            System.out.println(contentSummary.getLength());

            inputStream = fileSystem.open(readPath);
            System.out.println("文件size:"+String.valueOf(inputStream.available()/1000)+"k");

//            ContentSummary contentSummary = fileSystem.getContentSummary(readPath);
//            System.out.println(contentSummary);
//            System.out.println(contentSummary.getLength()/1024);

        } catch (IOException e) {
            e.printStackTrace();
        }

        FileOutputStream outputStream = null;
        try {
            outputStream = new FileOutputStream(new File(fileStorgePath));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        try {
//            IOUtils.copyBytes(inputStream, System.out, 4096, false);
            IOUtils.copyBytes(inputStream, outputStream, 4096, false);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(inputStream);
        }

    }

    public static void delete(String filePath) {
        FileSystem fileSystem = getFileSystem();
        Path path = new Path(filePath);
        try {
            if (fileSystem.exists(path)) {
                boolean delete = fileSystem.delete(path, true);
                System.out.println(delete);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void iteratorShowFiles(String path){
        FileSystem fileSystem = getFileSystem();
        iteratorShowFiles(fileSystem, new Path(path));
    }
    /**
     *
     * @param hdfs FileSystem 对象
     * @param path 文件路径
     */
    public static void iteratorShowFiles(FileSystem hdfs, Path path){
        try{
            if(hdfs == null || path == null){
                return;
            }
            //获取文件列表
            FileStatus[] files = hdfs.listStatus(path);

            //展示文件信息
            for (int i = 0; i < files.length; i++) {
                try{
                    if(files[i].isDirectory()){
                        System.out.println(">>>" + files[i].getPath()
                                + ", dir owner:" + files[i].getOwner());
                        //递归调用
                        iteratorShowFiles(hdfs, files[i].getPath());
                    }else if(files[i].isFile()){
                        System.out.println("   " + files[i].getPath()
                                + ", length:" + files[i].getLen()
                                + ", owner:" + files[i].getOwner());
                    }
                }catch(Exception e){
                    e.printStackTrace();
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public static void createDirs(String dirPath) {
        FileSystem fileSystem = getFileSystem();
        Path path = new Path(dirPath);
        try {
            if (fileSystem.exists(path)) {
                System.out.println("There is already exist " + path);
            } else {
                FsPermission filePermission = null;
//                filePermission = new FsPermission(
//                        FsAction.ALL, //user action
//                        FsAction.ALL, //group action
//                        FsAction.READ);//other action
                //创建目录 不设置权限,默认为当前hdfs服务器启动用户
                boolean success = fileSystem.mkdirs(path, null);

//                boolean success = fileSystem.mkdirs(path, filePermission);
                System.out.println(path + " is success " + success);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void renameDirPath(String oldDirPath, String newDirPath) {
        FileSystem fileSystem = getFileSystem();
        Path path = new Path(oldDirPath);
        try {
            if (!fileSystem.exists(path)) {
                System.out.println("There is no exist " + path);
            } else {
                fileSystem.rename(path, new Path(newDirPath));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //=============================================================================

    public static void main(String[] args) throws Exception {

        //=====================下载
//        String fileName = "/tmp/hadoop-yarn/staging/history/done_intermediate/cen/job_1497948413653_0001_conf.xml";

//        String fileName = "/571377719087006127.jpg";
//        String fileStorgePath = "d://test1.jpg";

//        String fileName = "/wordcount/input/text.txt";
//        String fileStorgePath = "d://test.txt";

//        String fileName = "/test/text.txt";
//        String fileStorgePath = "d://test1.txt";

//        String fileName = "/test";
//        String fileStorgePath = "d://test2.txt";

//        String fileName = "/test/test1/test.txt";
//        String fileStorgePath = "d://test3.txt";

        String fileName = "/test/test2/test1.jpg";
        String fileStorgePath = "d://test4.jpg";

        read(fileName, fileStorgePath);


        //=====================上传
//        String putFilePath = "/user/cen/output/file-output-test.xml";
//        String fromString putFilePath = "/user/cen/output/file-output-test.xml";

//        String putFilePath = "/test";
//        String fromFilePath = "d://test1.jpg";

//        String putFilePath = "/test";

//        String putFilePath = "/";
//        String fromFilePath = "d://test.txt";

//        String putFilePath = "/test/test2";
//        String fromFilePath = "d://test.txt";

//        String putFilePath = "/test/test1";
//        String fromFilePath = "d://test1.jpg";

//        uploadFile(fromFilePath, putFilePath);

        //=====================删除
//        String putFilePath = "/test/test1/test1.jpg";
//        delete(putFilePath);


        //=====================目录列表
//        String path = "/wordcount";

//        String path = "/test";
//
//        iteratorShowFiles(path);

        //=====================创建目录
//        String filePath = "/test/test1";
//        createDirs(filePath);

        //=====================修改目录
//        String oldDifPath = "/test/test1";
//        String newDirPath = "/test/test2";
//        renameDirPath(oldDifPath, newDirPath);




//        String filePath = "/test";
//        showDirsAndFiles(filePath);
    }

    public static void showDirsAndFiles(String filePath){
        FileSystem fileSystem = getFileSystem();
        Path path = new Path(filePath);
        try {
            if (!fileSystem.exists(path)) {
            } else {
                FileStatus[] fileStatuses = fileSystem.listStatus(path);
                for (FileStatus fileStatus : fileStatuses) {
                    System.out.println(fileStatus.getPath().getName());
                }

//                RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(path, false);
//                while (listFiles.hasNext()) {
//                    LocatedFileStatus fileStatus = listFiles.next();
//                    System.out.println(fileStatus.getPath().getName());
//                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
