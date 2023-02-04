package pl.tnt.kafkademo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

@SpringBootApplication
public class KafkaDemoApplication {

  public static void main(String[] args) {
    SpringApplication.run(KafkaDemoApplication.class, args);
  }

  @Data
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

  @Component
  @RequiredArgsConstructor
  static class KafkaSender {
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @SneakyThrows
    public void sendMessage(String topic, String key, Object body) {
      kafkaTemplate.send(topic, key, body).get();
    }
  }



}
