package br.com.alexandreaquiles.twitter2elastic;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ElasticSearchConsumer {

  private static Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);

  public static void main(String[] args) throws IOException, InterruptedException {
    RestHighLevelClient elasticSearchClient = createElasticSearchClient();

    CountDownLatch countDownLatch = new CountDownLatch(1);

    ConsumerRunnable runnable = new ConsumerRunnable(elasticSearchClient, countDownLatch);
    Thread thread = new Thread(runnable);
    thread.start();

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      logger.info("Vai desligar...");
      runnable.shutdown();
    }));

    countDownLatch.await();


  }

  private static RestHighLevelClient createElasticSearchClient() {

    String hostname = System.getenv("ELASTIC_SEARCH_HOSTNAME");
    String username = System.getenv("ELASTIC_SEARCH_USERNAME");
    String password = System.getenv("ELASTIC_SEARCH_PASSWORD");

    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

    RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https"))
      .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

    RestHighLevelClient client = new RestHighLevelClient(builder);
    return client;

  }
}

class ConsumerRunnable implements  Runnable {

  private static final Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);
  private final KafkaConsumer<String, String> consumer;
  private CountDownLatch countDownLatch;
  private RestHighLevelClient elasticSearchClient;

  ConsumerRunnable(RestHighLevelClient elasticSearchClient, CountDownLatch countDownLatch) {
    this.elasticSearchClient = elasticSearchClient;
    this.countDownLatch = countDownLatch;
    // http://kafka.apache.org/documentation/#consumerconfigs
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "tweets-consumers");
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    consumer = new KafkaConsumer<>(properties);

    consumer.subscribe(Collections.singleton("tweets"));
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
              + "key: " + record.key() + "\n"
          );

          String tweet = record.value();

          IndexRequest indexRequest = new IndexRequest("twitter")
            .source(tweet, XContentType.JSON);

          IndexResponse indexResponse = elasticSearchClient.index(indexRequest, RequestOptions.DEFAULT);
          String id = indexResponse.getId();

          logger.info("Elasticsearch id: " + id);

        }
      }

    } catch (WakeupException | IOException ex) {
        logger.info("Desligando...");
      try {
        consumer.close();
        elasticSearchClient.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      } finally {
        countDownLatch.countDown();
      }
    }


  }

  public void shutdown() {
    logger.info("Consumer is waking up...");
    consumer.wakeup();
  }
}
