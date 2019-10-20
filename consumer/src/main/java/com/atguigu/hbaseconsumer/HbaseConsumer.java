package com.atguigu.hbaseconsumer;

import com.atguigu.utils.PropertyUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.util.Collections;

public class HbaseConsumer {

    public static void main(String[] args) throws IOException {

        //创建Kafka消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(PropertyUtil.properties);

        consumer.subscribe(Collections.singletonList(PropertyUtil.getProperty("kafka.topics")));
        HbaseDAO hbaseDAO = new HbaseDAO();

        try {
            while (true) {
                //100ms获取一次topic数据
                ConsumerRecords<String, String> records = consumer.poll(100);

                for (ConsumerRecord<String, String> record : records) {
                    String line = record.value();
                    System.out.println(line);
                    hbaseDAO.puts(line);
                }
            }
        } finally {
            hbaseDAO.close();
        }
    }
}