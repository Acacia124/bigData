package yin.zhang.weather;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class WeatherPartitioner extends Partitioner<WeatherBo, IntWritable> {
    @Override
    public int getPartition(WeatherBo weatherBo, IntWritable intWritable, int i) {
        return weatherBo.getYear() % i;
    }
}
