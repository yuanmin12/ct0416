package com.atguigu.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * 根据传入的数据形成多组startRow和stopRow
 */
public class HBaseScanUtil {

    //2019-03,2019-06
    public static List<String[]> getRowKeys(String phoneNum, String start, String end) throws ParseException {

        //声明存放多组rowkey的集合
        ArrayList<String[]> rows = new ArrayList<>();

        //声明开始结束时间节点
        Calendar startPoint = Calendar.getInstance();
        Calendar endPoint = Calendar.getInstance();

        //
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM");

        Date startDate = sdf.parse(start);
        Date endDate = sdf.parse(end);

        //给日历类对象赋值
        startPoint.setTime(startDate);
        endPoint.setTime(endDate);


        //每个月分的结束标志
        Calendar monthPoint = Calendar.getInstance();
        monthPoint.setTime(startDate);
        monthPoint.add(Calendar.MONTH, 1);


        while (startPoint.getTime().getTime() <= endPoint.getTime().getTime()) {

            String[] rowkey = new String[2];

            String month = sdf.format(startPoint.getTime());
            String endMonth = sdf.format(monthPoint.getTime());

            //获取当前月份的分区号
            String parId = HbaseUtil.getParId(phoneNum, month, Integer.parseInt(PropertyUtil.getProperty("hbase.regions")));

            //拼接startstoprow
            String startRow = parId + "_" + phoneNum + "_" + month;
            String stopRow = parId + "_" + phoneNum + "_" + endMonth;

            rowkey[0] = startRow;
            rowkey[1] = stopRow;

            rows.add(rowkey);

            startPoint.add(Calendar.MONTH, 1);
            monthPoint.add(Calendar.MONTH, 1);
        }
        return rows;
    }
}
