package br.com.alexandreaquiles.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Consumer {

  private static final Logger logger = LoggerFactory.getLogger(Consumer.class);

  public static void main(String[] args) throws InterruptedException {

    CountDownLatch countDownLatch = new CountDownLatch(1);

    ConsumerRunnable runnable = new ConsumerRunnable(countDownLatch);
    Thread thread = new Thread(runnable);
    thread.start();

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      logger.info("Vai desligar...");
      runnable.shutdown();
    }));

    countDownLatch.await();

  }
}

class ConsumerRunnable implements  Runnable {

  private static final Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);
  private final KafkaConsumer<String, String> consumer;
  private CountDownLatch countDownLatch;

  ConsumerRunnable(CountDownLatch countDownLatch) {
    this.countDownLatch = countDownLatch;
    // http://kafka.apache.org/documentation/#consumerconfigs
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "meu-grupo");
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    consumer = new KafkaConsumer<>(properties);

    consumer.subscribe(Collections.singleton("first-topic"));
  }

  @Override
  public void run() {
    try {

      while (true) {
        // consumer.poll(100);
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<String, String> record : records) {
          logger.info(
            "topic: " + record.topic() + " "
              + "partition: " + record.partition() + " "
              + "offset: " + record.offset() + " "
              + "timestamp: " + record.timestamp() + "\n"
              + "key: " + record.key() + " "
              + "value: " + record.value() + "\n"
          );
        }
      }

    } catch ( WakeupException ex) {
      logger.info("Desligando...");
      consumer.close();
      countDownLatch.countDown();
    }


  }

  public void shutdown() {
    logger.info("Consumer is waking up...");
    consumer.wakeup();
  }
}
