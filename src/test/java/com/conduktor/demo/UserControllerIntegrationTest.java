package com.conduktor.demo;

import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.conduktor.demo.model.Content;
import com.conduktor.demo.model.UserData;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
@SpringBootTest
@AutoConfigureMockMvc
class UserControllerIntegrationTest {

  @Container
  static final KafkaContainer kafka =
      new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.7.0"));

  @DynamicPropertySource
  static void overrideProperties(DynamicPropertyRegistry registry) {
    registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    registry.add("user.data.file.path", () -> "static/random-people-test-data.json");
  }

  @Autowired private MockMvc mockMvc;

  @Autowired private ObjectMapper objectMapper;

  @RepeatedTest(2) // Tests repeated to assert offsets are not committed
  void givenDataIsLoadedInTopic_whenGetByTopicIsFetchedForAll_thenAllRecordShouldBeRetrieved()
      throws Exception {
    String baseUrl = "/api/v1/topic/%s/%d?count=%d";
    String topicName = "user-data";
    long offset = 0;
    int count = 10;

    String url = String.format(baseUrl, topicName, offset, count);
    ResultActions resultActions = mockMvc.perform(MockMvcRequestBuilders.get(url));
    resultActions.andExpect(status().isOk()).andExpect(jsonPath("$", hasSize(5)));
    List<UserData> actualUserData =
        objectMapper.readValue(
            resultActions.andReturn().getResponse().getContentAsString(),
            new TypeReference<List<UserData>>() {});
    List<UserData> expectedUserData = getUserData();
    Assertions.assertThat(actualUserData).isEqualTo(expectedUserData);
  }

  @Test
  void
      givenDataIsLoadedInTopic_whenGetByTopicIsFetchedForTwoRecords_thenTwoRecordShouldBeRetrieved()
          throws Exception {
    String baseUrl = "/api/v1/topic/%s/%d?count=%d";
    String topicName = "user-data";
    long offset = 0;
    int count = 2;

    String url = String.format(baseUrl, topicName, offset, count);
    ResultActions resultActions = mockMvc.perform(MockMvcRequestBuilders.get(url));
    resultActions.andExpect(status().isOk()).andExpect(jsonPath("$", hasSize(2)));
  }

  @Test
  void
      givenDataIsLoadedInTopic_whenGetByTopicIsFetchedForUnAvailableOffsets_thenNoRecordsShouldBeRetrieved()
          throws Exception {
    String baseUrl = "/api/v1/topic/%s/%d?count=%d";
    String topicName = "user-data";
    long offset = 100;
    int count = 10;

    String url = String.format(baseUrl, topicName, offset, count);
    ResultActions resultActions = mockMvc.perform(MockMvcRequestBuilders.get(url));
    resultActions.andExpect(status().isOk()).andExpect(jsonPath("$", hasSize(0)));
  }

  @ParameterizedTest
  @CsvSource({"-10,10", "10,-10"})
  void
      givenDataIsLoadedInTopic_whenGetByTopicIsFetchedForNegativeOffsetsOrLimit_thenNoRecordsShouldBeRetrieved(
          long offset, int count) throws Exception {
    String baseUrl = "/api/v1/topic/%s/%d?count=%d";
    String topicName = "user-data";

    String url = String.format(baseUrl, topicName, offset, count);
    mockMvc.perform(MockMvcRequestBuilders.get(url)).andExpect(status().isBadRequest());
  }

  @Test
  void
      givenDataIsLoadedInTopic_whenGetByTopicIsFetchedWithoutLimit_thenAtMost10RecordsShouldBeRetrieved()
          throws Exception {
    String baseUrl = "/api/v1/topic/%s/%d";
    String topicName = "user-data";
    String url = String.format(baseUrl, topicName, 0);
    ResultActions resultActions = mockMvc.perform(MockMvcRequestBuilders.get(url));
    resultActions
        .andExpect(status().isOk())
        .andExpect(jsonPath("$", hasSize(5))); // only 5 records available , all should be returned
  }

  public List<UserData> getUserData() throws IOException {
    File file = loadUserDataWithClassPathResource().getFile();
    String usersData = new String(Files.readAllBytes(file.toPath()));
    return objectMapper.readValue(usersData, Content.class).ctRoot();
  }

  public Resource loadUserDataWithClassPathResource() {
    return new ClassPathResource("static/random-people-test-data.json");
  }
}
