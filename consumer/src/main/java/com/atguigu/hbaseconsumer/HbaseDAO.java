package com.atguigu.hbaseconsumer;

import com.atguigu.constant.Constant;
import com.atguigu.utils.HbaseUtil;
import com.atguigu.utils.PropertyUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 1.创建命名空间
 * 2.创建表
 * 3.封装批量插入HBASE数据的puts方法***
 */
public class HbaseDAO {

    //命名空间名称
    private String ns = null;

    //表名
    private String tableName = null;

    //预分区数
    private int regions;

    //put集合
    private List<Put> puts;

    private Connection connection = null;

    private Table table;

    //主被叫区分
    private String flag;

    public HbaseDAO() throws IOException {
        //初始化相应的属性
        ns = PropertyUtil.getProperty("hbase.namespace");
        tableName = PropertyUtil.getProperty("hbase.table.name");
        regions = Integer.parseInt(PropertyUtil.getProperty("hbase.regions"));
        puts = new ArrayList<>();
        flag = "0";

        connection = ConnectionFactory.createConnection(Constant.CONF);
        table = connection.getTable(TableName.valueOf(tableName));

        //创建命名空间&表
        try {
            HbaseUtil.createNamespace(ns);
        } catch (Exception e) {
            System.out.println("命名空间已存在！！！");
        }
        HbaseUtil.createTable(tableName, PropertyUtil.getProperty("hbase.cf"), "f2");
    }

    public void puts(String line) throws IOException {

        //1.判断数据是否合法
        if (line.split(",").length < 4) {
            return;
        }

        //2.切割
        String[] splits = line.split(",");
        String call1 = splits[0];
        String call2 = splits[1];
        String buildTime = splits[2];
        String duration = splits[3];

        //3.封装Put对象

        //获取分区号
        String parId = HbaseUtil.getParId(call1, buildTime, regions);

        //拼接rowkey
        String row = HbaseUtil.getRow(parId, call1, buildTime, call2, flag, duration);

        //创建put对象
        Put put = new Put(Bytes.toBytes(row));

        //添加数据
        put.addColumn(Bytes.toBytes(PropertyUtil.getProperty("hbase.cf")), Bytes.toBytes("call1"), Bytes.toBytes(call1));
        put.addColumn(Bytes.toBytes(PropertyUtil.getProperty("hbase.cf")), Bytes.toBytes("call2"), Bytes.toBytes(call2));
        put.addColumn(Bytes.toBytes(PropertyUtil.getProperty("hbase.cf")), Bytes.toBytes("buildTime"), Bytes.toBytes(buildTime));
        put.addColumn(Bytes.toBytes(PropertyUtil.getProperty("hbase.cf")), Bytes.toBytes("flag"), Bytes.toBytes(flag));
        put.addColumn(Bytes.toBytes(PropertyUtil.getProperty("hbase.cf")), Bytes.toBytes("duration"), Bytes.toBytes(duration));

        //4.将put对象缓存到集合中
        puts.add(put);

        //5.根据集合大小，写到hbase，清空集合
        if (puts.size() >= 20) {
            table.put(puts);
            puts.clear();
        }
    }

    public void close() throws IOException {
        table.put(puts);
        table.close();
        connection.close();
    }

    public void timePut() throws IOException {
        table.put(puts);
    }


}
