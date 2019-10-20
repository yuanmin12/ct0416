import com.atguigu.constant.Constant;
import com.atguigu.utils.HBaseScanUtil;
import com.atguigu.utils.PropertyUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;

public class Test2 {

    public static void main(String[] args) throws IOException, ParseException {

        //获取连接&表对象
        Connection connection = ConnectionFactory.createConnection(Constant.CONF);
        Table table = connection.getTable(TableName.valueOf(PropertyUtil.getProperty("hbase.table.name")));

        //按照传入的数据获取每个月的startstoprow
        List<String[]> rowKeys = HBaseScanUtil.getRowKeys("19920860202", "2019-08", "2020-02");

        for (String[] rowKey : rowKeys) {
            System.out.println(rowKey[0]);
            System.out.println(rowKey[1]);
            System.out.println();
            //获取扫描对象
            Scan scan = new Scan(Bytes.toBytes(rowKey[0]), Bytes.toBytes(rowKey[1]));

            //获取数据并打印
            ResultScanner results = table.getScanner(scan);
            for (Result result : results) {
                System.out.println(Bytes.toString(result.getRow()));
            }
        }

        //关闭资源
        table.close();
        connection.close();
    }
}
