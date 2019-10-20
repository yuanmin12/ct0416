package com.atguigu.output;

import com.atguigu.Utils.JDBCUtil;
import com.atguigu.convertor.DimensionConvertor;
import com.atguigu.kv.key.CommDimension;
import com.atguigu.kv.value.CountDurationValue;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MysqlOutputFormat extends OutputFormat<CommDimension, CountDurationValue> {

    private FileOutputCommitter committer = null;

    @Override
    public RecordWriter<CommDimension, CountDurationValue> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {

        return new MysqlRecordWriter();
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {

    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        if (committer == null) {
            Path output = getOutputPath(context);
            committer = new FileOutputCommitter(output, context);
        }
        return committer;
    }

    public static Path getOutputPath(JobContext job) {
        String name = job.getConfiguration().get(FileOutputFormat.OUTDIR);
        return name == null ? null : new Path(name);
    }


    protected static class MysqlRecordWriter extends RecordWriter<CommDimension, CountDurationValue> {

        //声明
        String sql = null;
        DimensionConvertor dimensionConvertor;
        Connection connection;
        PreparedStatement preparedStatement;
        int cacheBount;
        int count;

        //构造方法（初始化相应属性）
        public MysqlRecordWriter() {
            sql = "INSERT INTO tb_call VALUES (?,?,?,?,?) ON DUPLICATE KEY UPDATE call_sum =?, call_duration_sum =?;";
            dimensionConvertor = new DimensionConvertor();

            connection = JDBCUtil.getInstance();
            try {
                connection.setAutoCommit(false);
                preparedStatement = connection.prepareStatement(sql);
            } catch (SQLException e) {
                e.printStackTrace();
            }

            //批量提交控制参数
            cacheBount = 500;
            count = 0;
        }

        /**
         * 核心写出方法（写往mysql）
         */
        @Override
        public void write(CommDimension key, CountDurationValue value) throws IOException, InterruptedException {

            //获取5个值
            int callCount = value.getCount();
            int callDuration = value.getDuration();

            //去mysql表里查询数据（联系人维度&时间维度ID）
            try {
                int contactID = dimensionConvertor.getDimensionID(key.getContactDimension());
                int dateID = dimensionConvertor.getDimensionID(key.getDateDimension());

                //拼接主键
                String priKey = contactID + "_" + dateID;

                //赋值
                int index = 0;
                preparedStatement.setString(++index, priKey);
                preparedStatement.setInt(++index, dateID);
                preparedStatement.setInt(++index, contactID);
                preparedStatement.setInt(++index, callCount);
                preparedStatement.setInt(++index, callDuration);
                preparedStatement.setInt(++index, callCount);
                preparedStatement.setInt(++index, callDuration);

                //添加到缓存
                preparedStatement.addBatch();
                count++;

                //提交（写到mysql）
                if (count >= cacheBount) {
                    preparedStatement.executeBatch();
                    connection.commit();
                    count = 0;
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }


        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {

            /**
             * 收尾工作
             * 1.提交残留在缓存的数据
             * 2.关闭相应的资源
             */
            try {
                preparedStatement.executeBatch();
                connection.commit();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            JDBCUtil.close(connection, preparedStatement, null);
        }
    }

}
