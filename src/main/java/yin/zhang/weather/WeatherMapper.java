package yin.zhang.weather;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class WeatherMapper extends Mapper<LongWritable, Text, WeatherBo, IntWritable> {

    WeatherBo weBo = new WeatherBo();
    IntWritable intVal = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = StringUtils.split(value.toString(), '\t');
        String[] date = StringUtils.split(words[0], '-');
        try {
            weBo.setYear(Integer.parseInt(date[0]));
            weBo.setMonth(Integer.parseInt(date[1]));
            weBo.setDay(Integer.parseInt(StringUtils.split(date[2], ' ')[0]));
            int wd = Integer.parseInt(words[1].substring(0, words[1].lastIndexOf("c")));
            weBo.setTemp(wd);
            intVal.set(wd);
            System.out.println("mapper-----------------> {" + weBo.toString() + "," + intVal.get() +"}");
            context.write(weBo, intVal);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
