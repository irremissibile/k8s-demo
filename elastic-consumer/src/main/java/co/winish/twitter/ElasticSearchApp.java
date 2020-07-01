package co.winish.twitter;

import co.winish.config.KafkaConfig;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class ElasticSearchApp {

    private final Logger logger = LoggerFactory.getLogger(ElasticSearchApp.class);

    private final String hostname = System.getenv("ELASTICSEARCH_HOSTNAME");
    private final int port = Integer.parseInt(System.getenv("ELASTICSEARCH_PORT"));
    private final String kafkaTopic = System.getenv("KAFKA_TOPIC");


    public static void main(String[] args) {
        new ElasticSearchApp().start();
    }


    private void start() {
        RestHighLevelClient client = initClient();
        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<String, String>(KafkaConfig.getStringExplicitCommitConsumerProperties("elasticsearch-app"));

        CountDownLatch latch = new CountDownLatch(1);

        logger.info("Creating the consumer thread");
        ElasticSearchConsumerRunnable myConsumerRunnable = new ElasticSearchConsumerRunnable(client, latch, consumer, kafkaTopic);

        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            myConsumerRunnable.shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        } finally {
            logger.info("Application is closing");
        }
    }


    public RestHighLevelClient initClient(){
        RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, port, "htpp"));

        return new RestHighLevelClient(builder);
    }
}
