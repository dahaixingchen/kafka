package com.dobest.concurrent;

import cn.hutool.core.io.file.FileReader;
import cn.hutool.core.io.file.FileWriter;
import cn.hutool.core.lang.Assert;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.File;
import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @Description:
 * @ClassName: KafkaServer
 * @Author chengfei
 * @DateTime 2021/5/13 16:05
 **/
public class KafkaConcurrentByTime {

    public static void main(String[] args) throws Exception {
        start();
        Thread.currentThread().join();
    }

    private static void start() throws Exception {

        //当天时间
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Date date = new Date();

        Properties pro = new Properties();
//        pro.load(new FileInputStream("F:\\kafka.properties"));
        pro.load(new FileInputStream("./kafka.properties"));

        FileReader reader = FileReader.create(new File(pro.getProperty("field-search-rootPath") + "field-search.properties"), StandardCharsets.UTF_8);
        FileWriter mfw = FileWriter.create(new File(pro.getProperty("merge-rootPath") + "merge_"+ dateFormat.format(date) +".txt"), StandardCharsets.UTF_8);

        Assert.notNull(pro.getProperty("topic"), "Topic 不能为空");
        Assert.notNull(pro.getProperty("start-times"), "时间区间不能为空");
        Assert.notNull(pro.getProperty("end-times"), "时间区间不能为空");

        Map<String, Object> configAdminClient = new HashMap<>();
        configAdminClient.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, pro.getProperty("bootstrap-servers"));
        AdminClient adminClient = KafkaAdminClient.create(configAdminClient);

        String topic = pro.getProperty("topic");
        DescribeTopicsResult topicsResult = adminClient.describeTopics(Arrays.asList(topic));
        KafkaFuture<Map<String, TopicDescription>> kafkaFuture = topicsResult.all();
        //拿到topic的每个partition
        TopicDescription topicDescription = kafkaFuture.get().get(topic);


        //读取过滤的字段
        List<String> lines = reader.readLines();
        lines.remove(0);

        //消费者配置
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, pro.getProperty("bootstrap-servers"));
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, pro.getProperty("group-id"));
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, pro.getProperty("max-poll-records"));//max.poll.records
        config.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, pro.getProperty("max-poll-interval-ms")); //max.poll.interval.ms
        config.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000"); //request.timeout.ms
        config.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "10000");//fetch.max.wait.ms
        config.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "5000000");//fetch.min.bytes
        config.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "50000000");//max.partition.fetch.bytes
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");


        //遍历每个分区，将不同分区的数据写入不同文件中
        topicDescription.partitions().forEach(partition -> {
            new Thread(() -> {
                Map<TopicPartition, Long> map = new HashMap<>();
                TopicPartition topicPartition = new TopicPartition(topic, partition.partition());
                KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config);
                map.put(topicPartition, Long.parseLong(pro.getProperty("start-times")));
                OffsetAndTimestamp offsetAndTimestamp = consumer.offsetsForTimes(map).get(topicPartition);

                //指定分区消费
                consumer.assign(Arrays.asList(topicPartition));
                //定位到特定的offset开始消费
                consumer.seek(topicPartition, offsetAndTimestamp.offset());

                FileWriter fw = FileWriter.create(
                        new File(pro.getProperty("partition-rootPath") + topicPartition.toString() + dateFormat.format(date) + ".txt")
                        , StandardCharsets.UTF_8);
                boolean isBreak = true;
                //拉取消息
                while (isBreak) {
                    ConsumerRecords<String, String> poll = consumer.poll(Duration.ofMillis(1000));
                    for (ConsumerRecord<String, String> record : poll) {
                        if (record.timestamp() > Long.parseLong(pro.getProperty("end-times"))) {
                            isBreak = false;
                            break;
                        }
                        String v = record.value();
                        // 读配置过滤信息

                        for (String filterValue : lines) {
                            if (v.contains(filterValue)) {
                                String result = String.format("filter_value %s topic: %s, partition %d, offset %d\n", filterValue, topic, partition.partition(), record.offset());
                                result = result + record.value();
                                System.err.println(result);
                                fw.write(record.value() + "\n", true);
                                mfw.write(record.value() + "\n", true);
                                break;
                            }
                        }
                    }
                }
            }).start();
        });
    }
}
