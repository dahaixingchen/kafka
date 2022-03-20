package com.feifei.group_offset

import java.util

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer

/**
  * @Description:
  * @ClassName: TestConsumer
  * @Author chengfei
  * @DateTime 2021/4/12 18:30
  **/
object TestConsumer {
  def main(args: Array[String]): Unit = {
    val kafkaConf = new util.HashMap[String, AnyRef]
    kafkaConf.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node01:9092")
    kafkaConf.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    kafkaConf.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    new KafkaConsumer[String, String](kafkaConf)
  }
}
