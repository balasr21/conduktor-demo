package com.conduktor.demo.service;

import com.conduktor.demo.model.UserData;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.LongStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class UserService {

  private final KafkaTemplate<String, String> kafkaTemplate;

  private final ObjectMapper objectMapper;

  private final DefaultKafkaConsumerFactory<String, String> consumerFactory;

  public UserService(
      KafkaTemplate<String, String> kafkaTemplate,
      DefaultKafkaConsumerFactory<String, String> consumerFactory,
      ObjectMapper objectMapper) {
    this.kafkaTemplate = kafkaTemplate;
    this.consumerFactory = consumerFactory;
    this.objectMapper = objectMapper;
  }

  public List<UserData> peek(String topicName, long offset, int limit) {
    List<UserData> userData = new ArrayList<>();
    kafkaTemplate
        .receive(fetchTopicPartitionOffset(topicName, offset, limit))
        .forEach(
            user -> {
              try {
                userData.add(objectMapper.readValue(user.value(), UserData.class));
              } catch (JsonProcessingException e) {
                log.error("topic.read.error with {}", e);
                throw new RuntimeException(e);
              }
            });
    log.info("topic.read.success for {}", topicName);
    return userData;
  }

  public List<TopicPartitionOffset> fetchTopicPartitionOffset(
      String topicName, long offset, int limit) {
    List<TopicPartitionOffset> topicPartitionOffsets = new ArrayList<>();
    try (KafkaConsumer<String, String> consumer =
        (KafkaConsumer<String, String>) consumerFactory.createConsumer()) {
      List<PartitionInfo> partitions = consumer.partitionsFor(topicName);
      partitions.stream()
          .forEach(
              partitionInfo -> {
                LongStream.range(offset, offset + Long.valueOf(limit))
                    .forEach(
                        offsetValue -> {
                          topicPartitionOffsets.add(
                              new TopicPartitionOffset(
                                  topicName, partitionInfo.partition(), offsetValue));
                        });
              });
    };
    return topicPartitionOffsets;
  }
}
