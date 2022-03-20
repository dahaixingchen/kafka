package com.feifei.group_offset;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class KafkaListenerServiceImpl {

    /**
     * 获取服务器上所有的groupId
     *
     * @return
     */
    public Set<String> getAllGroupId() {

        Map<String, Object> map = new HashMap<String, Object>();
        map.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "node01:9092");

        AdminClient client = AdminClient.create(map);

        Set<String> groupIds = new HashSet<String>();
        try {
            // 1.查询到所有的 groupId对象
            ListConsumerGroupsResult listConsumerGroupsResult = client.listConsumerGroups();
            KafkaFuture<Collection<ConsumerGroupListing>> allGroupId = listConsumerGroupsResult.all();
            allGroupId.get().forEach(group -> {
                groupIds.add(group.groupId());
            });
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } finally {
            client.close();
        }
        return groupIds;
    }

    /**
     * 获取具体消费情况
     *
     * @return
     */
    public List<KafkaConsumerDTO> getConsumerDetails(Set<String> groupIds) throws ExecutionException, InterruptedException {
        List<KafkaConsumerDTO> kafkaConsumerDtoS = new ArrayList<>();

        ArrayList<String> topics = new ArrayList<>();
        topics.add("bala");


        Map<String, Object> map = new HashMap<String, Object>();
        map.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "node01:9092");

        groupIds.forEach(groupId -> {
            AdminClient client = AdminClient.create(map);
            try {
                // 获取当前groupId消费情况
                Map<TopicPartition, OffsetAndMetadata> offsets = client.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();

                for (TopicPartition topicPartition : offsets.keySet()) {

                    String topic = topicPartition.topic();
                    int partition = topicPartition.partition();
                    OffsetAndMetadata offset = offsets.get(topicPartition);
                    long currentOffset = offset.offset();

                    // 数据组装
                    KafkaConsumerDTO kafkaConsumerDTO = new KafkaConsumerDTO();
                    kafkaConsumerDTO.setGroupId(groupId);
                    kafkaConsumerDTO.setTopic(topic);
                    kafkaConsumerDTO.setPartition(partition);
                    kafkaConsumerDTO.setCurrentOffset(currentOffset);

                    String metadata = offset.metadata();
                    System.out.println(kafkaConsumerDTO);
                    System.out.println(metadata);

                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            } finally {
                client.close();
            }
        });

        return kafkaConsumerDtoS;
    }


    /**
     * 获取该消费者组每个分区最后提交的偏移量
     *
     * @param consumer 消费者组对象
     */
    public Map<TopicPartition, Long> getLastOffset(KafkaConsumer consumer) {

        System.out.println("*************************************");
        List<TopicPartition> list = new ArrayList<>();

        String topic = "ooxx";

        List<PartitionInfo> partitions = consumer.partitionsFor(topic);
        partitions.forEach(partition -> {
            list.add(new TopicPartition(topic,partition.partition()));
        });

        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(list);
        endOffsets.forEach((k, v) -> {
            System.out.println(k.partition() + "**" + k.topic());
            System.out.println(k);
            System.out.println(v);
        });

        return endOffsets;
    }


    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Map<String, Object> map = new HashMap<String, Object>();
        map.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node01:9092");
        map.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        map.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());


        KafkaListenerServiceImpl kafkaListenerService = new KafkaListenerServiceImpl();
        Set<String> allGroupId = kafkaListenerService.getAllGroupId();
        List<KafkaConsumerDTO> consumerDetails = kafkaListenerService.getConsumerDetails(allGroupId);
        allGroupId.forEach(e -> System.out.println(e));

        kafkaListenerService.getLastOffset(new KafkaConsumer(map));
    }
}
