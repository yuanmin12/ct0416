package com.atguigu.mr;

import com.atguigu.kv.key.CommDimension;
import com.atguigu.output.MysqlOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CountDurationDriver extends Configuration implements Tool {

    private Configuration configuration = null;

    @Override
    public int run(String[] args) throws Exception {

        //获取连接&job对象
        Job job = Job.getInstance(configuration);
        job.setJarByClass(CountDurationDriver.class);

        //设置Mapper
        TableMapReduceUtil.initTableMapperJob("ns_telecom:calllog",
                new Scan(),
                CountDurationMapper.class,
                CommDimension.class,
                Text.class,
                job);

        //设置Reducer
        job.setReducerClass(CountDurationReducer.class);

        //设置outputformat
        job.setOutputFormatClass(MysqlOutputFormat.class);

        //提交
        boolean result = job.waitForCompletion(true);

        return result ? 0 : 1;
    }

    @Override
    public void setConf(Configuration conf) {
        configuration = conf;

    }

    @Override
    public Configuration getConf() {
        return configuration;
    }

    public static void main(String[] args) {

        Configuration configuration = HBaseConfiguration.create();

        int i = 1;
        try {
            i = ToolRunner.run(configuration, new CountDurationDriver(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.exit(i);
    }
}
