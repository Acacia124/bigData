package yin.zhang.friends;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * A A好友B A好友C A好友D
 */
public class FriendsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    Text tKey = new Text();
    IntWritable tVal = new IntWritable();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] users = StringUtils.split(value.toString(), ' ');
        for (int i = 1; i < users.length; i++) {
            tKey.set(getJoinName(users[0], users[i]));
            tVal.set(0);
            context.write(tKey, tVal);

            for (int j = i + 1; j < users.length; j++) {
                tKey.set(getJoinName(users[i], users[j]));
                tVal.set(1);
                context.write(tKey, tVal);
            }
        }
    }

    private String getJoinName(String a, String b) {
        return a.compareTo(b) > 0 ? b + ":" + a : a + ":" + b;
    }
}
