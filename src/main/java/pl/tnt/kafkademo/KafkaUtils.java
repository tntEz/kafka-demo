package pl.tnt.kafkademo;

import static java.util.Objects.isNull;
import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.springframework.kafka.support.serializer.JsonDeserializer.USE_TYPE_INFO_HEADERS;
import static org.springframework.kafka.support.serializer.JsonDeserializer.VALUE_DEFAULT_TYPE;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class KafkaUtils {

  private static final String AUTO_OFFSET_RESET_EARLIEST = "earliest";

  @Value("${spring.kafka.consumer.group-id}")
  private String consumerGroupId;
  @Value("${spring.kafka.properties.bootstrap.servers}")
  private String bootstrapServers;
  @Value("${spring.kafka.properties.security.protocol}")
  private String securityProtocol;
  @Value("${spring.kafka.message-listener.concurrency}")
  private int messageListenerConcurrency;

  public KafkaUtils() {
  }

  public <T> ConcurrentKafkaListenerContainerFactory<String, T> createListenerContainerFactory(Class<T> messageClass) {
    return createListenerContainerFactory(messageClass, null);
  }

  public <T> ConcurrentKafkaListenerContainerFactory<String, T> createListenerContainerFactory(Class<T> messageClass,
                                                                                               RetryListener retryListener) {
    DefaultErrorHandler errorHandler = new DefaultErrorHandler();
    ConcurrentKafkaListenerContainerFactory<String, T> factory = new ConcurrentKafkaListenerContainerFactory<>();

    if (isNull(retryListener)) {
      errorHandler.setRetryListeners(getRetryLogListener());
    } else {
      errorHandler.setRetryListeners(getRetryLogListener(), retryListener);
    }

    factory.setConsumerFactory(createConsumerFactory(messageClass));
    factory.setConcurrency(messageListenerConcurrency);
    factory.setCommonErrorHandler(errorHandler);

    return factory;
  }

  private <T> ConsumerFactory<String, T> createConsumerFactory(Class<T> messageClass) {
    Map<String, Object> consumerConfig = new HashMap<>();
    Map<String, Object> valueDeserializerConfig = new HashMap<>();
    JsonDeserializer<T> valueDeserializer = new JsonDeserializer<>();
    ErrorHandlingDeserializer<T> valueErrorHandlingDeserializer = new ErrorHandlingDeserializer<>(valueDeserializer);

    valueDeserializerConfig.put(VALUE_DEFAULT_TYPE, messageClass);
    valueDeserializerConfig.put(USE_TYPE_INFO_HEADERS, false);

    valueDeserializer.configure(valueDeserializerConfig, false);

    consumerConfig.put(BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
    consumerConfig.put(SECURITY_PROTOCOL_CONFIG, this.securityProtocol);
    consumerConfig.put(GROUP_ID_CONFIG, this.consumerGroupId);
    consumerConfig.put(AUTO_OFFSET_RESET_CONFIG, AUTO_OFFSET_RESET_EARLIEST);

    return new DefaultKafkaConsumerFactory<>(consumerConfig, new StringDeserializer(), valueErrorHandlingDeserializer);
  }

  private RetryListener getRetryLogListener() {
    return (consumerRecord, exception, deliveryAttempt) ->
        log.error("Oops... something went wrong when processing message. Attempt: {}.", deliveryAttempt, exception);
  }

}