package yin.zhang.word;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class CountMapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        IntWritable intWritable = new IntWritable(1);
        StringTokenizer iter = new StringTokenizer(line);
        while (iter.hasMoreTokens()) {
            word.set(iter.nextToken());
            context.write(word, intWritable);
        }
    }
}
