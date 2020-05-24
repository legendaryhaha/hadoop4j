package top.fsn.hadoop.service;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/***
 *
 * @Author:fsn
 * @Date: 2020/5/24 16:04
 * @Description
 */


public class MapReduceService {

    public static class SeparateMap extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // 读取一行数据
            String line = value.toString();
            // 因为英文字母是以" "为间隔的, 因此使用" "分隔符将一行数据切成多个单词并存在数组中
            String str[] = line.split(" ");
            // 循环迭代字符串，将一个单词变成<key,value>形式，及<"hello",1>
            for (String s : str) {
                context.write(new Text(s), new IntWritable(1));
            }

        }
    }

    public static class MergeReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable value : values) {
                count++;
            }
            context.write(key, new IntWritable(count));
        }
    }
}
