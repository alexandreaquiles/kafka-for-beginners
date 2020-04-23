package br.com.alexandreaquiles.kafka;


import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Producer {

  private static final Logger logger = LoggerFactory.getLogger(Producer.class);

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    // http://kafka.apache.org/documentation/#producerconfigs
    Properties properties = new Properties();
    //  properties.setProperty("bootstrap.servers", "localhost:9092");
    //  properties.setProperty("key.serializer", StringSerializer.class.getName());
    //  properties.setProperty("value.serializer", StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


    for (int i = 0; i < 10; i++) {

      String key = "id_" + i;

      ProducerRecord record = new ProducerRecord<>("first-topic", key, "Mensagem direto do Java: " + i);

      logger.info("key: " + record.key());

      producer.send(record, (recordMetadata, e) -> {
      if (e == null) {
        logger.info(
          "topic: " + recordMetadata.topic() + "\n"
            + "partition: " + recordMetadata.partition() + "\n"
            + "offset: " + recordMetadata.offset() + "\n"
            + "timestamp: " + recordMetadata.timestamp() + "\n"
        );
      } else {
        logger.error("Error while producing", e);
      }

      }).get();
    }

    //producer.flush();
    producer.close();

    /* SEMPRE A MESMA PARTITION! */
    /*
    [main] INFO br.com.alexandreaquiles.kafka.Producer - key: id_0
    [kafka-producer-network-thread | producer-1] INFO org.apache.kafka.clients.Metadata - [Producer clientId=producer-1] Cluster ID: ZsIN93GiQUOGZ6fqn8Dg3A
    [kafka-producer-network-thread | producer-1] INFO br.com.alexandreaquiles.kafka.Producer - topic: first-topic
    partition: 1
    offset: 4
    timestamp: 1587653288803

    [main] INFO br.com.alexandreaquiles.kafka.Producer - key: id_1
    [kafka-producer-network-thread | producer-1] INFO br.com.alexandreaquiles.kafka.Producer - topic: first-topic
    partition: 0
    offset: 6
    timestamp: 1587653288891

    [main] INFO br.com.alexandreaquiles.kafka.Producer - key: id_2
    [kafka-producer-network-thread | producer-1] INFO br.com.alexandreaquiles.kafka.Producer - topic: first-topic
    partition: 2
    offset: 10
    timestamp: 1587653288896

    [main] INFO br.com.alexandreaquiles.kafka.Producer - key: id_3
    [kafka-producer-network-thread | producer-1] INFO br.com.alexandreaquiles.kafka.Producer - topic: first-topic
    partition: 0
    offset: 7
    timestamp: 1587653288901

    [main] INFO br.com.alexandreaquiles.kafka.Producer - key: id_4
    [kafka-producer-network-thread | producer-1] INFO br.com.alexandreaquiles.kafka.Producer - topic: first-topic
    partition: 2
    offset: 11
    timestamp: 1587653288904

    [main] INFO br.com.alexandreaquiles.kafka.Producer - key: id_5
    [kafka-producer-network-thread | producer-1] INFO br.com.alexandreaquiles.kafka.Producer - topic: first-topic
    partition: 2
    offset: 12
    timestamp: 1587653288911

    [main] INFO br.com.alexandreaquiles.kafka.Producer - key: id_6
    [kafka-producer-network-thread | producer-1] INFO br.com.alexandreaquiles.kafka.Producer - topic: first-topic
    partition: 0
    offset: 8
    timestamp: 1587653288915

    [main] INFO br.com.alexandreaquiles.kafka.Producer - key: id_7
    [kafka-producer-network-thread | producer-1] INFO br.com.alexandreaquiles.kafka.Producer - topic: first-topic
    partition: 2
    offset: 13
    timestamp: 1587653288920

    [main] INFO br.com.alexandreaquiles.kafka.Producer - key: id_8
    [kafka-producer-network-thread | producer-1] INFO br.com.alexandreaquiles.kafka.Producer - topic: first-topic
    partition: 1
    offset: 5
    timestamp: 1587653288923

    [main] INFO br.com.alexandreaquiles.kafka.Producer - key: id_9
    [kafka-producer-network-thread | producer-1] INFO br.com.alexandreaquiles.kafka.Producer - topic: first-topic
    partition: 2
    offset: 14
    timestamp: 1587653288927
     */

    //Consumindo com:
    // kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic first-topic --from-beginning --group my-first-application
  }
}
