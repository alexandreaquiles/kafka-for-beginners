package br.com.alexandreaquiles.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
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
    // no group id
    // properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "meu-grupo");
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    consumer = new KafkaConsumer<>(properties);

    // don't subscribe
    // consumer.subscribe(Collections.singleton("first-topic"));

    // assign and seek => replay data or fetch a specific message
    TopicPartition topicPartition = new TopicPartition("first-topic", 0);
    consumer.assign(Collections.singleton(topicPartition));

    consumer.seek(topicPartition, 10L);

    /*
    [Thread-0] INFO br.com.alexandreaquiles.kafka.ConsumerRunnable - topic: first-topic partition: 0 offset: 10 timestamp: 1587492199732
    key: null value: Oi!

    [Thread-0] INFO br.com.alexandreaquiles.kafka.ConsumerRunnable - topic: first-topic partition: 0 offset: 11 timestamp: 1587493645862
    key: null value: oi

    [Thread-0] INFO br.com.alexandreaquiles.kafka.ConsumerRunnable - topic: first-topic partition: 0 offset: 12 timestamp: 1587497580686
    key: id_1 value: Mensagem direto do Java!

    [Thread-0] INFO br.com.alexandreaquiles.kafka.ConsumerRunnable - topic: first-topic partition: 0 offset: 13 timestamp: 1587497580686
    key: id_3 value: Mensagem direto do Java!

    [Thread-0] INFO br.com.alexandreaquiles.kafka.ConsumerRunnable - topic: first-topic partition: 0 offset: 14 timestamp: 1587497580686
    key: id_6 value: Mensagem direto do Java!

    [Thread-0] INFO br.com.alexandreaquiles.kafka.ConsumerRunnable - topic: first-topic partition: 0 offset: 15 timestamp: 1587498761729
    key: id_1 value: Mensagem direto do Java: 1

    [Thread-0] INFO br.com.alexandreaquiles.kafka.ConsumerRunnable - topic: first-topic partition: 0 offset: 16 timestamp: 1587498761729
    key: id_3 value: Mensagem direto do Java: 3

    [Thread-0] INFO br.com.alexandreaquiles.kafka.ConsumerRunnable - topic: first-topic partition: 0 offset: 17 timestamp: 1587498761733
    key: id_6 value: Mensagem direto do Java: 6

    [Thread-0] INFO br.com.alexandreaquiles.kafka.ConsumerRunnable - topic: first-topic partition: 0 offset: 18 timestamp: 1587499393338
    key: null value: Parece que não...

    [Thread-0] INFO br.com.alexandreaquiles.kafka.ConsumerRunnable - topic: first-topic partition: 0 offset: 19 timestamp: 1587500433690
    key: null value: Será que funciona?

    [Thread-0] INFO br.com.alexandreaquiles.kafka.ConsumerRunnable - topic: first-topic partition: 0 offset: 20 timestamp: 1587500464140
    key: null value: vamos testar isso daqui
     */

    /*
    Same as

    kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic first-topic --partition 0 --offset 10

    Oi!
    oi
    Mensagem direto do Java!
    Mensagem direto do Java!
    Mensagem direto do Java!
    Mensagem direto do Java: 1
    Mensagem direto do Java: 3
    Mensagem direto do Java: 6
    Parece que não...
    Será que funciona?
    vamos testar isso daqui

     */

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
