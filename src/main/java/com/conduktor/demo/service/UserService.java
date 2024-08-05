package com.conduktor.demo.service;

import com.conduktor.demo.config.DataSafeObjectMapper;
import com.conduktor.demo.model.UserData;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class UserService {

  private final DataSafeObjectMapper objectMapper;

  private final DefaultKafkaConsumerFactory<String, String> consumerFactory;

  public UserService(
      DefaultKafkaConsumerFactory<String, String> consumerFactory,
      DataSafeObjectMapper objectMapper) {
    this.consumerFactory = consumerFactory;
    this.objectMapper = objectMapper;
  }

  /**
   * This method is responsible for peeking into given topic and fetch records from all partitions
   * based on given offset and limit
   *
   * @param topicName
   * @param offset
   * @param limit
   * @return
   */
  public List<UserData> peek(String topicName, long offset, int limit) {

    List<UserData> userData = new ArrayList<>();

    try (KafkaConsumer<String, String> consumer =
        (KafkaConsumer<String, String>) consumerFactory.createConsumer()) {
      List<PartitionInfo> partitions = consumer.partitionsFor(topicName);

      for (PartitionInfo partition : partitions) {
        log.info("Collecting userdata from partition {}", partition.partition());
        int numberOfRecordsAdded = 0;
        seekForPartitionOffset(topicName, offset, partition, consumer);
        while (numberOfRecordsAdded < limit) {
          ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
          if (records.isEmpty()) {
            break; // Break current partition iteration if no records found at current offset
          }
          for (ConsumerRecord<String, String> consumerRecord : records) {
            if (numberOfRecordsAdded++ >= limit) break;
            userData.add(objectMapper.readValue(consumerRecord.value(), UserData.class));
          }
        }
      }
    }
    log.info(
        "Collected {} user data from topic {} with offset {} and limit of {}",
        userData.size(),
        topicName,
        offset,
        limit);
    return userData;
  }

  /**
   * Seeks to required offset in the given partition for further retrieval
   *
   * @param topicName
   * @param offset
   * @param partition
   * @param consumer
   */
  private static void seekForPartitionOffset(
      String topicName,
      long offset,
      PartitionInfo partition,
      KafkaConsumer<String, String> consumer) {
    TopicPartition topicPartition = new TopicPartition(topicName, partition.partition());
    consumer.assign(Collections.singletonList(topicPartition));
    consumer.seek(topicPartition, offset);
  }
}
