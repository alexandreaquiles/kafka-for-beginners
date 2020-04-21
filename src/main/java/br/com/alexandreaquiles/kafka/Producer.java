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

    ProducerRecord record = new ProducerRecord<>("first-topic", "Mensagem direto do Java!");

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

    //producer.flush();
    producer.close();

    //Consumindo com:
    // kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic first-topic --from-beginning --group my-first-application
  }
}
