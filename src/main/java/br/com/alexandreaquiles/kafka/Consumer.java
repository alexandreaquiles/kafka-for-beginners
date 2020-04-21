package br.com.alexandreaquiles.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer {

  private static final Logger logger = LoggerFactory.getLogger(Consumer.class);

  public static void main(String[] args) {
    // http://kafka.apache.org/documentation/#consumerconfigs
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "meu-grupo");
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

    consumer.subscribe(Collections.singleton("first-topic"));

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

        /*
        [main] INFO br.com.alexandreaquiles.kafka.Consumer - topic: first-topic partition: 1 offset: 0 timestamp: 1587482149717
        key: null value: Como você está?

        [main] INFO br.com.alexandreaquiles.kafka.Consumer - topic: first-topic partition: 1 offset: 1 timestamp: 1587490359068
        key: null value: a

        [main] INFO br.com.alexandreaquiles.kafka.Consumer - topic: first-topic partition: 1 offset: 2 timestamp: 1587490359953
        key: null value: b

        [main] INFO br.com.alexandreaquiles.kafka.Consumer - topic: first-topic partition: 1 offset: 3 timestamp: 1587490362634
        key: null value: e

        [main] INFO br.com.alexandreaquiles.kafka.Consumer - topic: first-topic partition: 1 offset: 4 timestamp: 1587490480622
        key: null value: 4

        [main] INFO br.com.alexandreaquiles.kafka.Consumer - topic: first-topic partition: 1 offset: 5 timestamp: 1587490481127
        key: null value: 5

        [main] INFO br.com.alexandreaquiles.kafka.Consumer - topic: first-topic partition: 1 offset: 6 timestamp: 1587490493452
        key: null value: 7

        [main] INFO br.com.alexandreaquiles.kafka.Consumer - topic: first-topic partition: 1 offset: 7 timestamp: 1587490493956
        key: null value: 8

        [main] INFO br.com.alexandreaquiles.kafka.Consumer - topic: first-topic partition: 1 offset: 8 timestamp: 1587490586304
        key: null value: 3

        [main] INFO br.com.alexandreaquiles.kafka.Consumer - topic: first-topic partition: 1 offset: 9 timestamp: 1587490623243
        key: null value: a

        [main] INFO br.com.alexandreaquiles.kafka.Consumer - topic: first-topic partition: 1 offset: 10 timestamp: 1587496155425
        key: null value: Mensagem direto do Java!

        [main] INFO br.com.alexandreaquiles.kafka.Consumer - topic: first-topic partition: 1 offset: 11 timestamp: 1587497580665
        key: id_0 value: Mensagem direto do Java!

        [main] INFO br.com.alexandreaquiles.kafka.Consumer - topic: first-topic partition: 1 offset: 12 timestamp: 1587497580690
        key: id_8 value: Mensagem direto do Java!

        [main] INFO br.com.alexandreaquiles.kafka.Consumer - topic: first-topic partition: 0 offset: 0 timestamp: 1587482155402
        key: null value: Tá na hora do almoço, né?

        [main] INFO br.com.alexandreaquiles.kafka.Consumer - topic: first-topic partition: 0 offset: 1 timestamp: 1587490340997
        key: null value: he

        [main] INFO br.com.alexandreaquiles.kafka.Consumer - topic: first-topic partition: 0 offset: 2 timestamp: 1587490437112
        key: null value: f

        [main] INFO br.com.alexandreaquiles.kafka.Consumer - topic: first-topic partition: 0 offset: 3 timestamp: 1587490482253
        key: null value: 6

        [main] INFO br.com.alexandreaquiles.kafka.Consumer - topic: first-topic partition: 0 offset: 4 timestamp: 1587490585081
        key: null value: 1

        [main] INFO br.com.alexandreaquiles.kafka.Consumer - topic: first-topic partition: 0 offset: 5 timestamp: 1587490585702
        key: null value: 2

        [main] INFO br.com.alexandreaquiles.kafka.Consumer - topic: first-topic partition: 0 offset: 6 timestamp: 1587490607987
        key: null value: 1

        [main] INFO br.com.alexandreaquiles.kafka.Consumer - topic: first-topic partition: 0 offset: 7 timestamp: 1587490608597
        key: null value: 2

        [main] INFO br.com.alexandreaquiles.kafka.Consumer - topic: first-topic partition: 0 offset: 8 timestamp: 1587490625022
        key: null value: b

        [main] INFO br.com.alexandreaquiles.kafka.Consumer - topic: first-topic partition: 0 offset: 9 timestamp: 1587490625970
        key: null value: c

        [main] INFO br.com.alexandreaquiles.kafka.Consumer - topic: first-topic partition: 0 offset: 10 timestamp: 1587492199732
        key: null value: Oi!

        [main] INFO br.com.alexandreaquiles.kafka.Consumer - topic: first-topic partition: 0 offset: 11 timestamp: 1587493645862
        key: null value: oi

        [main] INFO br.com.alexandreaquiles.kafka.Consumer - topic: first-topic partition: 0 offset: 12 timestamp: 1587497580686
        key: id_1 value: Mensagem direto do Java!

        [main] INFO br.com.alexandreaquiles.kafka.Consumer - topic: first-topic partition: 0 offset: 13 timestamp: 1587497580686
        key: id_3 value: Mensagem direto do Java!

        [main] INFO br.com.alexandreaquiles.kafka.Consumer - topic: first-topic partition: 0 offset: 14 timestamp: 1587497580686
        key: id_6 value: Mensagem direto do Java!

        [main] INFO br.com.alexandreaquiles.kafka.Consumer - topic: first-topic partition: 2 offset: 0 timestamp: 1587482145626
        key: null value: Bom dia, Kafka!

        [main] INFO br.com.alexandreaquiles.kafka.Consumer - topic: first-topic partition: 2 offset: 1 timestamp: 1587490337644
        key: null value: ha

        [main] INFO br.com.alexandreaquiles.kafka.Consumer - topic: first-topic partition: 2 offset: 2 timestamp: 1587490360791
        key: null value: c

        [main] INFO br.com.alexandreaquiles.kafka.Consumer - topic: first-topic partition: 2 offset: 3 timestamp: 1587490361601
        key: null value: d

        [main] INFO br.com.alexandreaquiles.kafka.Consumer - topic: first-topic partition: 2 offset: 4 timestamp: 1587490479042
        key: null value: 1

        [main] INFO br.com.alexandreaquiles.kafka.Consumer - topic: first-topic partition: 2 offset: 5 timestamp: 1587490479505
        key: null value: 2

        [main] INFO br.com.alexandreaquiles.kafka.Consumer - topic: first-topic partition: 2 offset: 6 timestamp: 1587490479950
        key: null value: 3

        [main] INFO br.com.alexandreaquiles.kafka.Consumer - topic: first-topic partition: 2 offset: 7 timestamp: 1587490494555
        key: null value: 9

        [main] INFO br.com.alexandreaquiles.kafka.Consumer - topic: first-topic partition: 2 offset: 8 timestamp: 1587490609142
        key: null value: 3

        [main] INFO br.com.alexandreaquiles.kafka.Consumer - topic: first-topic partition: 2 offset: 9 timestamp: 1587492203917
        key: null value: Como vai?

        [main] INFO br.com.alexandreaquiles.kafka.Consumer - topic: first-topic partition: 2 offset: 10 timestamp: 1587497093504
        key: null value: Mensagem direto do Java!

        [main] INFO br.com.alexandreaquiles.kafka.Consumer - topic: first-topic partition: 2 offset: 11 timestamp: 1587497158337
        key: null value: Mensagem direto do Java!

        [main] INFO br.com.alexandreaquiles.kafka.Consumer - topic: first-topic partition: 2 offset: 12 timestamp: 1587497580686
        key: id_2 value: Mensagem direto do Java!

        [main] INFO br.com.alexandreaquiles.kafka.Consumer - topic: first-topic partition: 2 offset: 13 timestamp: 1587497580686
        key: id_4 value: Mensagem direto do Java!

        [main] INFO br.com.alexandreaquiles.kafka.Consumer - topic: first-topic partition: 2 offset: 14 timestamp: 1587497580686
        key: id_5 value: Mensagem direto do Java!

        [main] INFO br.com.alexandreaquiles.kafka.Consumer - topic: first-topic partition: 2 offset: 15 timestamp: 1587497580690
        key: id_7 value: Mensagem direto do Java!

        [main] INFO br.com.alexandreaquiles.kafka.Consumer - topic: first-topic partition: 2 offset: 16 timestamp: 1587497580690
        key: id_9 value: Mensagem direto do Java!

         */

        /*
        kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group meu-grupo

        GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                               HOST            CLIENT-ID
        meu-grupo       first-topic     0          18              18              0               consumer-meu-grupo-1-ac148869-d86e-4b2c-8cc9-bbd3a394312d /127.0.0.1      consumer-meu-grupo-1
        meu-grupo       first-topic     1          16              16              0               consumer-meu-grupo-1-ac148869-d86e-4b2c-8cc9-bbd3a394312d /127.0.0.1      consumer-meu-grupo-1
        meu-grupo       first-topic     2          22              22              0               consumer-meu-grupo-1-ac148869-d86e-4b2c-8cc9-bbd3a394312d /127.0.0.1      consumer-meu-grupo-1

        */
      }


    }
  }
}
