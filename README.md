# Topic Peek Service

<hr>

This app is a REST service for retrieving records from a given topic, offset and limit of records

<hr>

### 1.1 Get Topic By TopicId

This endpoint retrieves user data from the given topic starting from given offset(inclusive) and limit the number of records requested

*GET /topic/{TOPIC_ID}/{OFFSET}?count=NoOfRecords*

Sample request and response body

#### Response Body

```
[
  {
    "name": "Florene Counts",
    "address": {
      "street": "3726 Brazenhose Lane",
      "town": "Bathgate",
      "postode": "NE1 0IY"
    },
    "telephone": "+964-6699-530-410",
    "pets": [
      "Milo",
      "Tucker"
    ],
    "score": 9.2,
    "email": "lenore.whyte@gmail.com",
    "url": "http://garcia.com",
    "description": "supplements competitions tabs investigated byte requesting golf slovakia expires pe adapted incorrect suited enlarge place celtic condition laura warrior spoke",
    "verified": true,
    "salary": 40674,
    "_id": "26T6YI88MQ8Y0U2H",
    "dob": "2020-06-05"
  }
]
```

<hr>

## 2. Technical Details:

### 2.1 Tools&Framework:

The below are the list of tools and framework used in the project!

* [SpringBoot 3.2.x](https://spring.io/projects/spring-boot) - The framework used
* [Maven](https://maven.apache.org/) - for Dependency Management
* [Java](https://www.oracle.com/technetwork/java/javase/overview/java8-2100321.html) - Java 21 as Programming language
* [Confluent-Kafka](https://hub.docker.com/r/confluentinc/cp-kafka/) - Confluent Kafka for broker and topic management
* [TestContainer](https://testcontainers.com/) - TestContainer for managing kafka broker and provide isolation for tests
* [Docker](https://www.docker.com/) - for running app in cloud

### 2.2 Key Features to highlight:

1. This app exposes a GET endpoint which accepts topic, offset and number of records and uses KafkaConsumer to receive records based on given condition
2. On starting the app, given test data for 500 random users will be loaded into the broker
3. Because of the springboot docker compose issue(highlighted more below in the section 3), docker compose needs to be brought up and down followed by starting/stopping the app. Once the issue is resolved, starting up the app will automatically up the docker
   compose and vice-versa during tear down
4. `log.cleanup.policy=delete` is the default config hence no explicit configurations are defined
5. `spring.kafka.consumer.enable-auto-commit` is set as per requirement so that the given endpoint can be requested for same details in an idempotent way
4. Comprehensive Unit tests and Integration tests(including test container for Kafka) are added for the endpoint and services

### 2.3 Solution & Assumptions

1. Given that there can be 3 partitions, each partition can have separate offset of messages.Since the endpoint is retrieving by giving an offset (without considering partition as its internal), the service retrieves data from all partitions and responds aggregated results but stops once the given limit is reached
2. This app loads test data using `KafkaTemplate`(instead of `KafkaProducer`) which internally uses `KafkaProducer`
3. Since the number of records `N` is optional, default limit of `10` is considered but could be Integer.MAX_VALUE as well(if we want to fetch all)
3. This APP is currently not secured by spring security, the endpoint can be accessed without any auth. This is left for future enhancements to give more priority to the changes

<hr>

## 3.Run Application

#### Pre-requisite

Please bring up the `confluent-kafka` instance by running the docker script in class path file name(docker-compose.yml). 

This step is required until a spring issue(https://stackoverflow.com/questions/77385146/springboot-docker-error-cannot-invoke-dockercliinspectresponse-hostconfig-b) is resolved which is currently not compatible to run docker compose scripts

```
docker-compose up -d    
```

This application is preconfigured with required properties and doesn't require any external properties to start. Once docker containers are up, give few seconds for topics to be created

#### Starting the App

Below command can be used to invoke the application

mvn spring-boot:run

<hr>

## 4. Future Enhancements

1. As of this submission, docker compose scripts needs to be manually run up and down. It would be nice to embed this as part of app start and tear down
2. Caching - For the given Topic, Offset - we can cache the response
   For the subsequent requests if number of records is within our stored range we can filter and retrieve it.
   If not we can retrieve from broker and update cache

   ```
   Request 1 : test_topic/0?count=10 -> Cache the response
   Request 2 : test_topic/0?count=5  -> retrieve from cache
   Request 3 : test_topic/0?count=50 -> Update cache by making request to kafka API from offset 0 or make requests for offset starting from 11 and then update cache
   ``` 

3. Add spring security for the project(may be use IAM)
4. Expose API using Swagger/OpenAPI
5. Log configuration
6. Handling exceptions by defining error models which contains attributes for granular error details
