package com.atguigu.mr;

import com.atguigu.kv.key.CommDimension;
import com.atguigu.kv.value.CountDurationValue;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CountDurationReducer extends Reducer<CommDimension, Text, CommDimension, CountDurationValue> {

    private CountDurationValue countDurationValue = new CountDurationValue();

    @Override
    protected void reduce(CommDimension key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        //通话时长&通话次数的初始化
        int countSum = 0;
        int durationSum = 0;

        //循环累加
        for (Text value : values) {
            String durationStr = value.toString();

            countSum++;
            durationSum += Integer.parseInt(durationStr);
        }

        //给value赋值
        countDurationValue.setCount(countSum);
        countDurationValue.setDuration(durationSum);

        //将数据写出
        context.write(key, countDurationValue);
    }
}
