package com.jjhu.hadoop.mapreduce;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by jjhu on 2014/12/1.
 */
public class AutoForbiddenMr {

    public static class AutoForbiddenMapper extends Mapper {
        @Override
        protected void map(Object key, Object value, Context context) throws IOException, InterruptedException {
            super.map(key, value, context);
        }
    }

    public static class AutoForbiddenReducer extends Reducer{
        @Override
        protected void reduce(Object key, Iterable values, Context context) throws IOException, InterruptedException {
            super.reduce(key, values, context);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        JobConf statisticConf = new JobConf();
//        statisticConf.addResource("hdfs://192.168.134.128:9000/conf/core-site.xml");
//        statisticConf.addResource("hdfs://192.168.134.128:9000/conf/hdfs-site.xml");
        /*Job statisticJob= new Job(statisticConf);
        statisticJob.setJarByClass(AutoForbiddenMr.class);
        statisticJob.setMapperClass(RecordStatisticsMr.RechargeMapper.class);
        statisticJob.setMapperClass(RecordStatisticsMr.UserActionMapper.class);
        statisticJob.setReducerClass(RecordStatisticsMr.StatisticsReducer.class);
        statisticJob.setOutputKeyClass(IntWritable.class);
        statisticJob.setOutputValueClass(IntWritable.class);
        String in = "hdfs://192.168.134.128:9000/data/sortInput.txt";
        String out = "hdfs://192.168.134.128:9000/output";
        FileSystem fs = FileSystem.get(statisticConf);
        Path inPath = new Path(in);
        Path  outPath = new Path(out);
        if(fs.exists(outPath)){
            fs.delete(outPath, true);
            System.out.println("输出路径存在，已删除!");
        }
        FileInputFormat.addInputPath(statisticJob, inPath);
        FileOutputFormat.setOutputPath(statisticJob, outPath);

        Configuration mainConf = new Configuration();
        Job mainJob = new Job(mainConf);
        mainJob.setJarByClass(AutoForbiddenMr.class);
        mainJob.setMapperClass(AutoForbiddenMapper.class);
        mainJob.setReducerClass(AutoForbiddenReducer.class);
        mainJob.setOutputKeyClass(NullWritable.class);
        mainJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(mainJob, outPath);
        FileOutputFormat.setOutputPath(mainJob, new Path(args[0]));*/

        Job job1 = new Job(statisticConf);

    }
}
