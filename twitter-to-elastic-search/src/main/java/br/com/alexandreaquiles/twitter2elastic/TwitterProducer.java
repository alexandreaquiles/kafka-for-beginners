package br.com.alexandreaquiles.twitter2elastic;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

  private static Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

  public static void main(String[] args) {
    BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100);

    Client twitterClient = setupTwitterClient(msgQueue, "#COVID19");
    KafkaProducer<String, String> producer = setUpKafkaProducer();

    twitterClient.connect();

    try {
      while (!twitterClient.isDone()) {
        String tweet = msgQueue.poll(5, TimeUnit.SECONDS);
        logger.info(tweet);

        ProducerRecord record = new ProducerRecord<>("tweets", tweet);
        producer.send(record, (metadata, exception) -> {
          if (exception != null) {
            logger.error("Error sending tweet to Kafka", exception);
          }
        });
      }
    } catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    } finally {
      twitterClient.stop();
      producer.close();
    }
  }

  private static Client setupTwitterClient(BlockingQueue<String> msgQueue, String term) {
    Hosts hosts = new HttpHosts(Constants.STREAM_HOST);
    StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
    List<String> terms = Lists.newArrayList(term);
    endpoint.trackTerms(terms);

    String consumerKey = System.getenv("TWITTER_HBC_CLIENT_API_KEY");
    String consumerSecret = System.getenv("TWITTER_HBC_CLIENT_API_KEY_SECRET");
    String token = System.getenv("TWITTER_HBC_CLIENT_ACCESS_TOKEN");
    String secret = System.getenv("TWITTER_HBC_CLIENT_ACCESS_TOKEN_SECRET");
    Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

    Client client = new ClientBuilder()
      .hosts(hosts)
      .authentication(hosebirdAuth)
      .endpoint(endpoint)
      .processor(new StringDelimitedProcessor(msgQueue))
      .build();
    return client;
  }

  private static KafkaProducer<String, String> setUpKafkaProducer() {
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
    return producer;
  }

}
