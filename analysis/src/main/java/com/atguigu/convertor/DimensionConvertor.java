package com.atguigu.convertor;

import com.atguigu.Utils.JDBCUtil;
import com.atguigu.Utils.LRUCache;
import com.atguigu.kv.base.BaseDimension;
import com.atguigu.kv.key.ContactDimension;
import com.atguigu.kv.key.DateDimension;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 根据传入的维度信息（联系人维度&时间维度）获取维度id
 */
public class DimensionConvertor {

    //声明一个缓存对象
    private LRUCache lruCache = new LRUCache(5000);

    //获取JDBC的连接
    private Connection connection = JDBCUtil.getInstance();

    public int getDimensionID(BaseDimension baseDimension) throws SQLException {

        //获取缓存数据的key
        String lruKey = getLruKey(baseDimension);

        //读缓存数据，如果存在，则直接返回
        if (lruCache.containsKey(lruKey)) {
            return lruCache.get(lruKey);
        }

        //获取对应维度的sql语句
        String[] sqls = getSqls(baseDimension);

        //第一次查询
        //如果查询不到数据，则插入数据
        //第二次查询
        int id = execSql(connection, sqls, baseDimension);
        if (id == 0) {
            throw new RuntimeException("未找到匹配维度信息！！！");
        }

        //将获取的结果数据写入缓存
        lruCache.put(lruKey, id);

        return id;
    }

    //执行SQL，返回维度对应id
    private synchronized int execSql(Connection connection, String[] sqls, BaseDimension baseDimension) throws SQLException {

        PreparedStatement preparedStatement = null;

        //第一次查询
        preparedStatement = connection.prepareStatement(sqls[0]);
        setArgument(preparedStatement, baseDimension);
        ResultSet resultSet = preparedStatement.executeQuery();
        if (resultSet.next()) {
            return resultSet.getInt(1);
        }

        //插入操作
        preparedStatement = connection.prepareStatement(sqls[1]);
        setArgument(preparedStatement, baseDimension);
        preparedStatement.executeUpdate();

        //第二次查询
        preparedStatement = connection.prepareStatement(sqls[0]);
        setArgument(preparedStatement, baseDimension);
        resultSet = preparedStatement.executeQuery();
        if (resultSet.next()) {
            return resultSet.getInt(1);
        }

        JDBCUtil.close(null, preparedStatement, resultSet);

        return 0;
    }

    //给预编译SQL赋值
    private void setArgument(PreparedStatement preparedStatement, BaseDimension baseDimension) throws SQLException {
        int index = 0;
        if (baseDimension instanceof ContactDimension) {
            //给联系人维度sql赋值
            ContactDimension contactDimension = (ContactDimension) baseDimension;
            preparedStatement.setString(++index, contactDimension.getPhoneNum());
            preparedStatement.setString(++index, contactDimension.getName());
        } else {
            //给时间维度SQL赋值
            DateDimension dateDimension = (DateDimension) baseDimension;
            preparedStatement.setInt(++index, Integer.parseInt(dateDimension.getYear()));
            preparedStatement.setInt(++index, Integer.parseInt(dateDimension.getMonth()));
            preparedStatement.setInt(++index, Integer.parseInt(dateDimension.getDay()));
        }
    }

    //生成sql语句
    private String[] getSqls(BaseDimension baseDimension) {
        String[] sqls = new String[2];

        //根据不同维度封装不同的sql语句（2条）
        if (baseDimension instanceof ContactDimension) {
            sqls[0] = "SELECT `id` FROM `tb_contacts` WHERE `telephone`=? and `name` =?;";
            sqls[1] = "INSERT INTO `tb_contacts` VALUES(NULL,?,?);";
        } else {
            sqls[0] = "SELECT `id` FROM `tb_dimension_date` WHERE `year`=? and `month` =? and `day`=?;";
            sqls[1] = "INSERT INTO `tb_dimension_date` VALUES(NULL,?,?,?);";
        }
        return sqls;
    }

    private String getLruKey(BaseDimension baseDimension) {

        StringBuilder sb = new StringBuilder();

        //根据不同维度拼接相应的key
        if (baseDimension instanceof ContactDimension) {
            ContactDimension contactDimension = (ContactDimension) baseDimension;
            sb.append(contactDimension.getPhoneNum());
        } else {
            DateDimension dateDimension = (DateDimension) baseDimension;
            sb.append(dateDimension.getYear()).append(dateDimension.getMonth()).append(dateDimension.getDay());
        }
        return sb.toString();
    }


}
