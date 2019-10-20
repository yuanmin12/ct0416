package com.atguigu.comprocess;

import com.atguigu.constant.Constant;
import com.atguigu.utils.HbaseUtil;
import com.atguigu.utils.PropertyUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class CalleeComprocess extends BaseRegionObserver {

    private int regions = Integer.parseInt(PropertyUtil.getProperty("hbase.regions"));

    //插入被叫数据
    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {

        //获取协处理中的表
        String tableName = e.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString();
        //获取当前操作的表
        String currentTable = PropertyUtil.getProperty("hbase.table.name");
        if (!tableName.equals(currentTable)) {
            return;
        }

        //取出上一条数据的rowkey
        String oldRowKey = Bytes.toString(put.getRow());

        //切割
        String[] split = oldRowKey.split("_");

        if ("1".equals(split[4])) {
            return;
        }

        String caller = split[1];
        String buildTime = split[2];
        String callee = split[3];
        String duration = split[5];

        //生成put对象
        String parId = HbaseUtil.getParId(callee, buildTime, regions);
        String newRow = HbaseUtil.getRow(parId, callee, buildTime, caller, "1", duration);

        Put newPut = new Put(Bytes.toBytes(newRow));

        //添加数据
        newPut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("call1"), Bytes.toBytes(callee));
        newPut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("buildTime"), Bytes.toBytes(buildTime));
        newPut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("call2"), Bytes.toBytes(caller));
        newPut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("flag"), Bytes.toBytes("1"));
        newPut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("duration"), Bytes.toBytes(duration));

        //获取连接&表
        Connection connection = ConnectionFactory.createConnection(Constant.CONF);
        Table table = connection.getTable(TableName.valueOf(tableName));

        //插入数据
        table.put(newPut);

        //关闭资源
        table.close();
        connection.close();
    }
}
