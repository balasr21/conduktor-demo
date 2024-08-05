package com.conduktor.demo.config;

import com.conduktor.demo.exception.LoadDataException;
import com.conduktor.demo.model.Content;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class LoadUserData implements SmartInitializingSingleton {

  private final KafkaTemplate<String, String> kafkaTemplate;

  private final DataSafeObjectMapper objectMapper;

  private final String topicName;

  private final String userDataFilePath;

  public LoadUserData(
      KafkaTemplate<String, String> kafkaTemplate,
      @Value("${user.data.topic.name}") String topicName,
      @Value("${user.data.file.path}") String userDataFilePath,
      DataSafeObjectMapper objectMapper) {
    this.kafkaTemplate = kafkaTemplate;
    this.topicName = topicName;
    this.userDataFilePath = userDataFilePath;
    this.objectMapper = objectMapper;
  }

  /**
   * This method is invoked right before the application is ready. It loads data into a Kafka topic
   * synchronously before the port is exposed.
   */
  @Override
  public void afterSingletonsInstantiated() {
    log.info("data.load.start");
    try {
      getUserData()
          .ctRoot()
          .forEach(
              userData -> {
                try {
                  var recordMetadata =
                      kafkaTemplate
                          .send(topicName, userData.id(), objectMapper.writeValueAsString(userData))
                          .get()
                          .getRecordMetadata();
                  log.info(
                      "data.load.success for user {} in partition {} with offset {}",
                      userData.id(),
                      recordMetadata.partition(),
                      recordMetadata.offset());
                } catch (ExecutionException | InterruptedException e) {
                  log.info("data.load.error for user {}", userData.id());
                  Thread.currentThread().interrupt();
                  throw new LoadDataException("InterruptedException: " + e.getMessage(), e);
                }
              });
      log.info("data.load.complete");
    } catch (IOException e) {
      log.warn("Error while loading data from classpath");
      throw new LoadDataException("IOException: " + e.getMessage(), e);
    }
  }

  public Content getUserData() throws IOException {
    File file = loadUserDataWithClassPathResource().getFile();
    String usersData = new String(Files.readAllBytes(file.toPath()));
    return objectMapper.readValue(usersData, Content.class);
  }

  public Resource loadUserDataWithClassPathResource() {
    return new ClassPathResource(userDataFilePath);
  }
}
