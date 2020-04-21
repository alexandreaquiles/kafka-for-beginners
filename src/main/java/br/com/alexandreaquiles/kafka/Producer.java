package br.com.alexandreaquiles.kafka;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer {
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

    producer.send(record);

    //producer.flush();
    producer.close();

    //Consumindo com:
    // kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic first-topic --from-beginning --group my-first-application
  }
}
