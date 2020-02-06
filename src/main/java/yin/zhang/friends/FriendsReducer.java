package yin.zhang.friends;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FriendsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    IntWritable tVal = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int num = 0;
        for (IntWritable value : values) {
            if(value.get() == 0) {
                return;
            }
            num++;
        }
        tVal.set(num);
        context.write(key, tVal);
    }
}
