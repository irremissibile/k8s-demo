package co.winish.twitter;

import co.winish.config.KafkaConfig;
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
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    private final Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

    private final String consumerKey = System.getenv("TWITTER_CONSUMER_API_KEY");
    private final String consumerSecret = System.getenv("TWITTER_CONSUMER_API_KEY_SECRET");
    private final String accessKey = System.getenv("TWITTER_ACCESS_TOKEN");
    private final String accessSecret = System.getenv("TWITTER_ACCESS_TOKEN_SECRET");

    private final String kafkaTopic = System.getenv("KAFKA_TWITTER_TWEETS_TOPIC");

    private final List<String> twitterTopics = Arrays.asList(System.getenv("TWITTER_TOPICS").split(","));


    public static void main(String[] args) {
        new TwitterProducer().run();
    }


    public void run() {
        BlockingQueue<String> messageQueue = new LinkedBlockingQueue<String>(1000);
        Client client = initTwitterClient(messageQueue);
        client.connect();

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(KafkaConfig.getBatchStringProducerProperties());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping twitter client");
            client.stop();
            logger.info("Closing Kafka producer");
            producer.close();
            logger.info("Everything's stopped");
        }));


        while (!client.isDone()) {
            String message = null;

            try {
                message = messageQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }

            if (message != null){
                logger.info(message);
                producer.send(new ProducerRecord<>(kafkaTopic, message), (recordMetadata, e) -> {
                    if (e != null)
                        logger.error(e.getMessage());
                });
            }
        }

        logger.info("End of an application");
    }


    private Client initTwitterClient(BlockingQueue<String> messageQueue) {
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        hosebirdEndpoint.trackTerms(twitterTopics);

        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, accessKey, accessSecret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(messageQueue));

        return builder.build();
    }

}
