package com.mr.findfriends;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;

public class FindFriendsReduce extends Reducer<Text, Text, Text, Text> {

    // 用map存储已经遍历过的用户，和该用户对应的朋友们
    Map<String, Set<String>> userWithFriends = new HashMap<>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        Set<String> myFriends = new HashSet<>();
        // 记录本次用户所有的朋友
        for (Text myFriend : values) {
            myFriends.add(String.valueOf(myFriend));
        }

        // 遍历之前的用户
        for (String user : userWithFriends.keySet()) {
            Set<String> commonFriends = myFriends.stream().filter(userWithFriends.get(user)::contains).collect(Collectors.toSet());
            if (!commonFriends.isEmpty()) {
                String outKey = user + "-" + key;
                StringJoiner sj = new StringJoiner(" ");
                for (String comFriend : commonFriends) {
                    sj.add(comFriend);
                }
                context.write(new Text(outKey), new Text(sj.toString()));
            }
        }

        // 将本次用户计入记录
        userWithFriends.put(String.valueOf(key), myFriends);

    }

}
