package pl.tnt.kafkademo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@SpringBootApplication
public class KafkaDemoApplication {

  public static final String SOME_DTO_TOPIC = "some.dto.topic";

  public static void main(String[] args) {
    SpringApplication.run(KafkaDemoApplication.class, args);
  }

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  static class SomeDto {
    private String propA;
    private String propB;
  }

  @Configuration
  @RequiredArgsConstructor
  static class KafkaConfig {

    private final KafkaUtils kafkaUtils;

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, SomeDto> someDtoContainerFactory() {
      return this.kafkaUtils.createListenerContainerFactory(SomeDto.class);
    }
  }

  @Slf4j
  @Component
  @RequiredArgsConstructor
  static class KafkaConsumer {

    @KafkaListener(topics = SOME_DTO_TOPIC, containerFactory = "someDtoContainerFactory")
    public void consume(SomeDto someDto) {
      log.info("Received: {}", someDto);
    }

    @KafkaListener(id="some-app-consumer-group-2", topics = SOME_DTO_TOPIC, containerFactory = "someDtoContainerFactory")
    public void consume2(SomeDto someDto) {
      log.info("Received-2: {}", someDto);
    }
  }

  @Component
  @RequiredArgsConstructor
  static class KafkaSender {
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @PostConstruct
    void setUp() {
      sendMessage(SOME_DTO_TOPIC, "key", new SomeDto("propA", "propB"));
    }

    @SneakyThrows
    public void sendMessage(String topic, String key, Object body) {
      kafkaTemplate.send(topic, key, body).get();
    }
  }
}
