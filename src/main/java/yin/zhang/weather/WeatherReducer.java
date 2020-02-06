package yin.zhang.weather;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WeatherReducer extends Reducer<WeatherBo, IntWritable, Text, IntWritable> {
    Text text = new Text();
    IntWritable intVal = new IntWritable();
    int flag = 0;
    int day = 0;

    @Override
    protected void reduce(WeatherBo key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        System.out.println("reducer-----------------> " + key.toString());
        for (IntWritable value : values) {
            if (flag == 0) {
                text.set(key.toString());
                intVal.set(value.get());
                context.write(text, intVal);
                flag++;
                day = key.getDay();
                continue;
            }

            if (flag != 0 && day != key.getDay()) {
                text.set(key.toString());
                intVal.set(value.get());
                context.write(text, intVal);
                return;
            }
        }
    }
}