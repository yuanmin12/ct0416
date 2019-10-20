package com.atguigu.mr;

import com.atguigu.kv.key.CommDimension;
import com.atguigu.kv.key.ContactDimension;
import com.atguigu.kv.key.DateDimension;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CountDurationMapper extends TableMapper<CommDimension, Text> {

    private Map<String, String> contacts = new HashMap<>();
    private CommDimension commDimension = new CommDimension();
    private Text v = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        contacts.put("15369468720", "李雁");
        contacts.put("19920860202", "卫艺");
        contacts.put("18411925860", "仰莉");
        contacts.put("14473548449", "陶欣悦");
        contacts.put("18749966182", "施梅梅");
        contacts.put("19379884788", "金虹霖");
        contacts.put("19335715448", "魏明艳");
        contacts.put("18503558939", "华贞");
        contacts.put("13407209608", "华啟倩");
        contacts.put("15596505995", "仲采绿");
        contacts.put("17519874292", "卫丹");
        contacts.put("15178485516", "戚丽红");
        contacts.put("19877232369", "何翠柔");
        contacts.put("18706287692", "钱溶艳");
        contacts.put("18944239644", "钱琳");
        contacts.put("17325302007", "缪静欣");
        contacts.put("18839074540", "焦秋菊");
        contacts.put("19879419704", "吕访琴");
        contacts.put("16480981069", "沈丹");
        contacts.put("18674257265", "褚美丽");
        contacts.put("18302820904", "孙怡");
        contacts.put("15133295266", "许婵");
        contacts.put("17868457605", "曹红恋");
        contacts.put("15490732767", "吕柔");
        contacts.put("15064972307", "冯怜云");
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

        //获取rowkey
        String rowkey = Bytes.toString(key.get());

        String[] split = rowkey.split("_");

        //只获取主叫数据
        if ("1".equals(split[4])) {
            return;
        }

        //获取rowkey原始数据
        String caller = split[1];
        //2019-08-23 12:21:12
        String buildTime = split[2];
        String callee = split[3];
        String duration = split[5];

        //设置value的值
        v.set(duration);

        String year = buildTime.substring(0, 4);
        String month = buildTime.substring(5, 7);
        String day = buildTime.substring(8, 10);

        //主叫数据
        ContactDimension callerDimension = new ContactDimension();
        callerDimension.setPhoneNum(caller);
        callerDimension.setName(contacts.get(caller));

        commDimension.setContactDimension(callerDimension);

        //年维度
        DateDimension yearDimension = new DateDimension(year, "-1", "-1");
        commDimension.setDateDimension(yearDimension);
        context.write(commDimension, v);

        //月维度
        DateDimension monthDimension = new DateDimension(year, month, "-1");
        commDimension.setDateDimension(monthDimension);
        context.write(commDimension, v);

        //日维度
        DateDimension dayDimension = new DateDimension(year, month, day);
        commDimension.setDateDimension(dayDimension);
        context.write(commDimension, v);

        //被叫数据
        ContactDimension calleeDimension = new ContactDimension();
        calleeDimension.setPhoneNum(callee);
        calleeDimension.setName(contacts.get(callee));
        commDimension.setContactDimension(calleeDimension);

        //年维度
        commDimension.setDateDimension(yearDimension);
        context.write(commDimension, v);

        //月维度
        commDimension.setDateDimension(monthDimension);
        context.write(commDimension, v);

        //日维度
        commDimension.setDateDimension(dayDimension);
        context.write(commDimension, v);
    }
}
