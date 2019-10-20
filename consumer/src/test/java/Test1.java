import com.atguigu.constant.Constant;
import com.atguigu.utils.HBaseFilterUtil;
import com.atguigu.utils.PropertyUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 19920860202 2019-04，2019-06
 */
public class Test1 {

    public static void main(String[] args) throws IOException {

        //获取号码列过滤器（2个）
        Filter filter1 = HBaseFilterUtil.eqFilter("f1", "call1", Bytes.toBytes("19920860202"));
        Filter filter2 = HBaseFilterUtil.eqFilter("f1", "call2", Bytes.toBytes("19920860202"));
        Filter filter3 = HBaseFilterUtil.orFilter(filter1, filter2);

        //时间上的过滤
        Filter filter4 = HBaseFilterUtil.gtFilter("f1", "buildTime", Bytes.toBytes("2019-04"));
        Filter filter5 = HBaseFilterUtil.ltFilter("f1", "buildTime", Bytes.toBytes("2019-07"));
        Filter filter6 = HBaseFilterUtil.andFilter(filter4, filter5);

        Filter filter = HBaseFilterUtil.andFilter(filter3, filter6);

        //获取连接
        Connection connection = ConnectionFactory.createConnection(Constant.CONF);

        //获取表对象
        Table table = connection.getTable(TableName.valueOf(PropertyUtil.getProperty("hbase.table.name")));

        //创建scan对象
        Scan scan = new Scan();

        //设置过滤规则
        scan.setFilter(filter);

        //获取数据并打印
        ResultScanner results = table.getScanner(scan);
        for (Result result : results) {
            System.out.println(Bytes.toString(result.getRow()));
        }
    }
}