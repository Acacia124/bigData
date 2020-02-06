package yin.zhang.weather;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WeatherJob {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 1、构建Job
        job.setJarByClass(WeatherJob.class);
        job.setJobName("Weather");

        // 2、设置输入输出路径
        Path inPath = new Path("/root/weather.txt");
        FileInputFormat.addInputPath(job, inPath);
        Path outPath = new Path("/weather");
        if (outPath.getFileSystem(conf).exists(outPath)) {
            outPath.getFileSystem(conf).delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, outPath);

        // 3、设置Mapper
        job.setMapperClass(WeatherMapper.class);
        job.setMapOutputKeyClass(WeatherBo.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 4、自定义比较器（年月正序， 温度倒序)
        job.setSortComparatorClass(WeatherSortComparator.class);

        // 5、自定义分区器
        job.setPartitionerClass(WeatherPartitioner.class);
        // 6、设置reduce分区数量
        job.setNumReduceTasks(2);

        // 7、自定义组排序(相同年月为1组)
        job.setGroupingComparatorClass(WeatherGroupingComparator.class);

        // 8、设置reduce
        job.setReducerClass(WeatherReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.waitForCompletion(true);
    }
}
