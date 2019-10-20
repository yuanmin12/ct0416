package com.atguigu.utils;

import com.atguigu.constant.Constant;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.DecimalFormat;

/**
 * 1.创建命名空间
 * 2.判断表是否存在
 * 3.创建表
 * 4.分区健生成
 * 5.RowKey设计
 */
public class HbaseUtil {

    public static void createNamespace(String ns) throws IOException {

        //获取连接&Admin对象
        Connection connection = ConnectionFactory.createConnection(Constant.CONF);
        Admin admin = connection.getAdmin();

        //创建命名空间描述器
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(ns).build();

        //创建命名空间
        admin.createNamespace(namespaceDescriptor);

        //关闭资源
        admin.close();
        connection.close();
    }

    //判断表是否存在
    private static boolean tableExist(String tableName) throws IOException {

        //获取连接&Admin
        Connection connection = ConnectionFactory.createConnection(Constant.CONF);
        Admin admin = connection.getAdmin();

        //判断
        boolean exists = admin.tableExists(TableName.valueOf(tableName));

        //关闭资源
        admin.close();
        connection.close();

        //返回true或者false
        return exists;
    }

    //创建表
    public static void createTable(String tableName, String... cfs) throws IOException {

        //获取连接&Admin
        Connection connection = ConnectionFactory.createConnection(Constant.CONF);
        Admin admin = connection.getAdmin();

        //判断表是否存在
        if (tableExist(tableName)) {
            return;
        }

        //创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        hTableDescriptor.addCoprocessor("com.atguigu.comprocess.CalleeComprocess");

        //循环添加列描述器
        for (String cf : cfs) {
            //创建列描述器
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }

        int regions = Integer.parseInt(PropertyUtil.getProperty("hbase.regions"));

        //创建表操作
        admin.createTable(hTableDescriptor, getSplits(regions));

        //关闭资源
        admin.close();
        connection.close();
    }

    //分区健生成【00|，01|，02|，03|。。。】
    private static byte[][] getSplits(int regions) {

        //声明二维数组
        byte[][] splits = new byte[regions][];

        DecimalFormat df = new DecimalFormat("00");

        //赋值
        for (int i = 0; i < regions; i++) {
            splits[i] = Bytes.toBytes(df.format(i) + "|");
        }

        //返回
        return splits;
    }

    //rowkey设计
    public static String getRow(String parID, String call1, String buildTime, String call2, String flag, String duration) {

        return parID + "_" +
                call1 + "_" +
                buildTime + "_" +
                call2 + "_" +
                flag + "_" +
                duration;
    }

    //分区号生成[00,01,02...]
    public static String getParId(String call1, String buildTime, int regions) {

        //取手机号后4位
        String last4Num = call1.substring(7);
        //取年月
        String yearMonth = buildTime.replace("-", "").substring(0, 6);

        int hashCode = (Integer.parseInt(last4Num) ^ Integer.parseInt(yearMonth)) % regions;

        return new DecimalFormat("00").format(hashCode);
    }

}
