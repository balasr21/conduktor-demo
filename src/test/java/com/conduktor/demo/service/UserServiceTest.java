package com.conduktor.demo.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.conduktor.demo.config.DataSafeObjectMapper;
import com.conduktor.demo.model.UserData;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ExtendWith(SpringExtension.class)
class UserServiceTest {

  @Spy
  DataSafeObjectMapper dataSafeObjectMapper =
      new DataSafeObjectMapper(new ObjectMapper().registerModule(new JavaTimeModule()));

  @Mock private DefaultKafkaConsumerFactory<String, String> consumerFactory;

  @InjectMocks private UserService userService;

  private KafkaConsumer<String, String> consumer;

  @BeforeEach
  public void setUp() {
    consumer = mock(KafkaConsumer.class);
  }

  @Test
  void testPeekSinglePartition() {
    String topicName = "test-topic";
    long offset = 0;
    int limit = 2;

    when(consumerFactory.createConsumer()).thenReturn(consumer);
    when(consumer.partitionsFor(topicName))
        .thenReturn(List.of(new PartitionInfo(topicName, 0, null, null, null)));
    TopicPartition topicPartition = new TopicPartition(topicName, 0);
    when(consumer.endOffsets(List.of(topicPartition))).thenReturn(Map.of(topicPartition, 2l));

    ConsumerRecords<String, String> consumerRecords1 =
        getConsumerRecord(topicName, "368YCC2THQH1HEAQ");
    ConsumerRecords<String, String> consumerRecords2 =
        getConsumerRecord(topicName, "S94NE1SQQDEY14G6");

    when(consumer.poll(ArgumentMatchers.any(Duration.class)))
        .thenReturn(consumerRecords1)
        .thenReturn(consumerRecords2);
    List<UserData> result = userService.peek(topicName, offset, limit);
    assertEquals(2, result.size());
    assertEquals("368YCC2THQH1HEAQ", result.get(0).id());
    assertEquals("S94NE1SQQDEY14G6", result.get(1).id());
  }

  private static @NotNull ConsumerRecords<String, String> getConsumerRecord(
      String topicName, String key) {
    Map<TopicPartition, List<ConsumerRecord<String, String>>> recordsMap;

    ConsumerRecord<String, String> record2 =
        new ConsumerRecord<>(
            topicName,
            0,
            1L,
            key,
            "{\"_id\":\"%s\",\"name\":\"Nana Tilley\",\"dob\":\"2017-05-23\",\"address\":{\"street\":\"1065 Capesthorne Road\",\"town\":\"Ferryhill\",\"postode\":\"IP05 4YQ\"},\"telephone\":\"+507-5229-696-588\",\"pets\":[\"Nala\",\"Bailey\"],\"score\":3.3,\"email\":\"alphonso-ochs4@yahoo.com\",\"url\":\"https://buried.com\",\"description\":\"my colleagues beats slow breeding espn discrimination link para calculated interaction dutch agrees relatively hamburg advertise broke editor characteristic adopted\",\"verified\":true,\"salary\":14985}"
                .formatted(key));

    recordsMap = new HashMap<>();
    recordsMap.put(new TopicPartition(topicName, 0), Arrays.asList(record2));
    ConsumerRecords<String, String> consumerRecords2 = new ConsumerRecords<>(recordsMap);
    return consumerRecords2;
  }

  private static @NotNull ConsumerRecords<String, String> getConsumerRecord_1(String topicName) {
    ConsumerRecord<String, String> record1 =
        new ConsumerRecord<>(
            topicName,
            0,
            0L,
            "368YCC2THQH1HEAQ",
            "{\"_id\":\"368YCC2THQH1HEAQ\",\"name\":\"Kiana Yoo\",\"dob\":\"2021-05-31\",\"address\":{\"street\":\"2745 Shaftsbury Circle\",\"town\":\"Slough\",\"postode\":\"LS67 1ID\"},\"telephone\":\"+39-3380-033-155\",\"pets\":[\"Sadie\",\"Rosie\"],\"score\":3.7,\"email\":\"palma14@hotmail.com\",\"url\":\"http://earth.com\",\"description\":\"strips rt administrators composer mumbai warranty tribunal excited halo costumes surgery series spare ticket monitored whose criminal screens enrollment range\",\"verified\":true,\"salary\":31900}");

    Map<TopicPartition, List<ConsumerRecord<String, String>>> recordsMap = new HashMap<>();
    recordsMap.put(new TopicPartition(topicName, 0), Arrays.asList(record1));
    ConsumerRecords<String, String> consumerRecords1 = new ConsumerRecords<>(recordsMap);
    return consumerRecords1;
  }
}
