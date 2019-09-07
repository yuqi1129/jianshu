package com.yuqi.jianshu.hadoop;

/**
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.StringTokenizer;
 */

/**
 * Author yuqi
 * Time 30/8/19
 **/
public class HoopTest {
    /**
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
                InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                //累加每一个value
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance();

        //设置本次job作业使用的mapper类是那个
        job.setJarByClass(HoopTest.class);
        //本次job作业使用的mapper类是那个？
        job.setMapperClass(TokenizerMapper.class);
        //本次job作业使用的reducer类是那个
        job.setCombinerClass(IntSumReducer.class);
        //本次job作业使用的reducer类是那个
        job.setReducerClass(IntSumReducer.class);
        //本次job作业使用的reducer类的输出数据key类型
        job.setOutputKeyClass(Text.class);
        //本次job作业使用的reducer类的输出数据value类型
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(1);//设置reduce的个数


        //判断output文件夹是否存在，如果存在则删除
        Path path = new Path(otherArgs[1]);
        FileSystem fileSystem =path.getFileSystem(conf);//根据path找到这个文件夹
        if(fileSystem.exists(path)) {
            fileSystem.delete(path,true);//true的意思是，就算output里面有东西，也一带删除
        }

        //本次job作业要处理的原始数据所在的路径
        FileInputFormat.addInputPath(conf, new Path(otherArgs[0]));

        //本次job作业产生的结果输出路径
        FileOutputFormat.setOutputPath(conf, new Path(otherArgs[1]));


        //提交本次作业
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    */
}
