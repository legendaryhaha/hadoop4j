package top.fsn.hadoop.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/***
 *
 * @Author:fsn
 * @Date: 2020/5/24 10:51
 * @Description
 */

@Component
public class HadoopService {
    @Value("${hdfs.key}")
    private String key;
    @Value("${hdfs.path}")
    private String path;
    @Value("${hdfs.userName}")
    private String userName;

    private String savePath = "/hdfs/data";


    /**
     * 获取HDFS配置信息
     * @return
     */
    private Configuration getConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set(key, path);
        return configuration;
    }

    /**
     * 获取HDFS文件系统对象
     * @return
     * @throws Exception
     */
    public FileSystem getFileSystem() {
        // 客户端去操作hdfs时是有一个用户身份的，默认情况下hdfs客户端api
        // 会从jvm中获取一个参数作为自己的用户身份DHADOOP_USER_NAME=hadoop
        // 也可以在构造客户端fs对象时，通过参数传递进去
        try {
            return FileSystem.get(new URI(path), getConfiguration(), userName);
        } catch (IOException | InterruptedException | URISyntaxException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     *
     * @param tmpFile   上传的文件对象
     * @return          true: 成功 false: 失败
     */

    public boolean upload(File tmpFile) {
        if (tmpFile == null) {
            return false;
        }
        FileSystem system = null;
        try {
            // 初始化FileSystem
            system = getFileSystem();
        } catch (Exception e) {
            e.printStackTrace();
        }
        // 源文件的路径
        Path src = new Path(tmpFile.getPath());
        // 目标文件的路径
        Path target = new Path(savePath + File.separator + tmpFile.getName());
        System.out.println(savePath + File.separator + tmpFile.getName());
        if (system == null) {
            return false;
        }
        // 调用文件系统的文件复制方法
        try {
            system.copyFromLocalFile(src, target);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    /**
     * 展示文件列表
     * @return  List<Map<String, String>>, 其中map里的key: fileName, value: filePath
     */
    public List<Map<String, String>> list() {
        FileSystem system = getFileSystem();
        Path src = new Path(savePath);
        // true表示递归找出所有文件
        RemoteIterator<LocatedFileStatus> list = null;
        List<Map<String, String>> newList = new ArrayList<>();
        try {
            list = system.listFiles(src, true);
            while (list.hasNext()) {
                LocatedFileStatus next = list.next();
                Path filePath = next.getPath();
                String fileName = filePath.getName();
                Map<String, String> map = new HashMap<>();
                map.put("fileName", fileName);
                map.put("filePath", filePath.toString());
                newList.add(map);
            }
            system.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return newList;
    }

    /**
     *
     * @param name  待下载文件名称
     * @return      文件的输入流
     */
    public InputStream down(String name) {
        FileSystem system = getFileSystem();
        FSDataInputStream inputStream = null;
        try {
            inputStream = system.open(new Path(savePath + File.separator + name));
            // system.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return inputStream;
    }


    public boolean count(String name, String target) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = getConfiguration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(HadoopService.class);
        job.setJobName("count task");

        // 设置实现了map函数的类
        job.setMapperClass(MapReduceService.SeparateMap.class);
        // 设置实现了reduce函数的类
        job.setReducerClass(MapReduceService.MergeReduce.class);

        // 设置reduce函数的key值
        job.setOutputKeyClass(Text.class);
        // 设置reduce函数的value值
        job.setOutputValueClass(IntWritable.class);

        // 设置读取文件的路径，都是从HDFS中读取
        FileInputFormat.addInputPath(job, new Path(savePath + File.separator + name));
        // 设置mapreduce程序的输出路径，MapReduce的结果都是输入到文件中, 这里直接输入到result目录中
        FileOutputFormat.setOutputPath(job,new Path(savePath + File.separator + target));

        return job.waitForCompletion(true) ? true : false;
    }
}
