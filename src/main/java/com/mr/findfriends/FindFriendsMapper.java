package com.mr.findfriends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FindFriendsMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String data = value.toString();
        String[] userAndFriends = data.split(":");
        Text user = new Text(userAndFriends[0]);
        String[] friends = userAndFriends[1].split(",");
        for (String friend : friends) {
            context.write(user, new Text(friend));
        }
    }

}
