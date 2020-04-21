package br.com.alexandreaquiles.kafka;


import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer {

  private static final Logger logger = LoggerFactory.getLogger(Producer.class);

  public static void main(String[] args) {
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

      });
    }

    /*
    kafka-producer-network-thread | producer-1] INFO br.com.alexandreaquiles.kafka.Producer - topic: first-topic
    partition: 1
    offset: 11
    timestamp: 1587497580665

    [kafka-producer-network-thread | producer-1] INFO br.com.alexandreaquiles.kafka.Producer - topic: first-topic
    partition: 1
    offset: 12
    timestamp: 1587497580690

    [kafka-producer-network-thread | producer-1] INFO br.com.alexandreaquiles.kafka.Producer - topic: first-topic
    partition: 0
    offset: 12
    timestamp: 1587497580686

    [kafka-producer-network-thread | producer-1] INFO br.com.alexandreaquiles.kafka.Producer - topic: first-topic
    partition: 0
    offset: 13
    timestamp: 1587497580686

    [kafka-producer-network-thread | producer-1] INFO br.com.alexandreaquiles.kafka.Producer - topic: first-topic
    partition: 0
    offset: 14
    timestamp: 1587497580686

    [kafka-producer-network-thread | producer-1] INFO br.com.alexandreaquiles.kafka.Producer - topic: first-topic
    partition: 2
    offset: 12
    timestamp: 1587497580686

    [kafka-producer-network-thread | producer-1] INFO br.com.alexandreaquiles.kafka.Producer - topic: first-topic
    partition: 2
    offset: 13
    timestamp: 1587497580686

    [kafka-producer-network-thread | producer-1] INFO br.com.alexandreaquiles.kafka.Producer - topic: first-topic
    partition: 2
    offset: 14
    timestamp: 1587497580686

    [kafka-producer-network-thread | producer-1] INFO br.com.alexandreaquiles.kafka.Producer - topic: first-topic
    partition: 2
    offset: 15
    timestamp: 1587497580690

    [kafka-producer-network-thread | producer-1] INFO br.com.alexandreaquiles.kafka.Producer - topic: first-topic
    partition: 2
    offset: 16
    timestamp: 1587497580690
     */

    //producer.flush();
    producer.close();

    //Consumindo com:
    // kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic first-topic --from-beginning --group my-first-application
  }
}
