package com.conduktor.demo.service;

import com.conduktor.demo.config.DataSafeObjectMapper;
import com.conduktor.demo.model.UserData;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
      int numberOfRecordsAdded = 0;

      for (PartitionInfo partition : partitions) {
        log.info("Collecting userdata from partition {}", partition.partition());
        long lastOffsetRecordInPartition =
            seekForPartitionOffsetAndGetLastOffsetRecord(topicName, offset, partition, consumer);
        while (isRecordsAvailableAfterOffset(offset, lastOffsetRecordInPartition)
            && isRecordAddedUnderLimit(numberOfRecordsAdded, lastOffsetRecordInPartition, limit)) {
          ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

          for (ConsumerRecord<String, String> consumerRecord : records) {
            if (numberOfRecordsAdded++ >= limit) break;
            userData.add(objectMapper.readValue(consumerRecord.value(), UserData.class));
          }
        }
      }
    }
    log.info(
        "Collected {} user data from topic {} with offset {} and count of {}",
        userData.size(),
        topicName,
        offset,
        limit);
    return userData;
  }

  private boolean isRecordAddedUnderLimit(
      int numberOfRecordsAdded, long lastOffsetRecordInPartition, int limit) {
    return numberOfRecordsAdded < limit && (lastOffsetRecordInPartition + 1) > numberOfRecordsAdded;
  }

  private boolean isRecordsAvailableAfterOffset(long offset, long lastOffsetRecordInPartition) {
    return lastOffsetRecordInPartition > 0 && offset < lastOffsetRecordInPartition;
  }

  /**
   * Seeks to required offset in the given partition for further retrieval
   *
   * @param topicName
   * @param offset
   * @param partition
   * @param consumer
   */
  private static long seekForPartitionOffsetAndGetLastOffsetRecord(
      String topicName,
      long offset,
      PartitionInfo partition,
      KafkaConsumer<String, String> consumer) {
    TopicPartition topicPartition = new TopicPartition(topicName, partition.partition());
    consumer.assign(Collections.singletonList(topicPartition));
    consumer.seek(topicPartition, offset);
    Map<TopicPartition, Long> endOffset =
        consumer.endOffsets(List.of(new TopicPartition(topicName, partition.partition())));
    return !endOffset.isEmpty() ? endOffset.get(topicPartition) - 1 : -1;
  }
}
