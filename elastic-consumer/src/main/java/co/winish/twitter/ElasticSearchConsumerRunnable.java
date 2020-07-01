package co.winish.twitter;

import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;


public class ElasticSearchConsumerRunnable implements Runnable {

    private final Logger logger = LoggerFactory.getLogger(ElasticSearchConsumerRunnable.class);
    private final RestHighLevelClient client;
    private final CountDownLatch latch;
    private final KafkaConsumer<String, String> consumer;
    private final String topic;


    public ElasticSearchConsumerRunnable(RestHighLevelClient client, CountDownLatch latch,
                                         KafkaConsumer<String, String> consumer, String topic) {
        this.client = client;
        this.latch = latch;
        this.consumer = consumer;
        this.topic = topic;
        consumer.subscribe(Collections.singletonList(topic));
    }


    public void run() {
        try {
            while(true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                int recordCount = records.count();
                logger.info("Received " + recordCount + " records");

                BulkRequest bulkRequest = new BulkRequest();

                for (ConsumerRecord<String, String> record : records){
                    try {
                        String id = extractTweetId(record.value());

                        IndexRequest indexRequest = new IndexRequest("tweets")
                                .source(record.value(), XContentType.JSON)
                                .id(id);

                        bulkRequest.add(indexRequest);
                    } catch (NullPointerException e){
                        logger.warn("Something bad happened with: " + record.value());
                    }

                }

                if (recordCount > 0) {
                    try {
                        BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);

                        logger.info("Committing offsets...");
                        consumer.commitSync();
                        logger.info("Offsets have been committed");

                    } catch (IOException e) {
                        logger.error(e.getMessage());
                    }
                }
            }
        } catch (WakeupException e) {
            logger.info("Received shutdown signal");
        } finally {
            consumer.close();

            try {
                client.close();
            } catch (IOException e) {
                logger.error("Couldn't close the client");
                logger.error(e.getMessage());
            }

            latch.countDown();
        }
    }


    private String extractTweetId(String tweetJson){
        return JsonParser.parseString(tweetJson)
                        .getAsJsonObject()
                        .get("id_str")
                        .getAsString();
    }


    public void shutdown() {
        consumer.wakeup();
    }
}
