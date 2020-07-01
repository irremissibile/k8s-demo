package co.winish.twitter;

import co.winish.config.KafkaStreamsConfig;
import com.google.gson.JsonParser;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class StreamsTweetsFilter {

    private final Logger logger = LoggerFactory.getLogger(StreamsTweetsFilter.class);

    private final String inputKafkaTopic = System.getenv("KAFKA_TWITTER_TWEETS_TOPIC");
    private final String outputKafkaTopic = System.getenv("KAFKA_FILTERED_TWEETS_TOPIC");

    private final List<String> shouldBePresent = Arrays.asList(System.getenv("FILTER_KEYWORDS").split(","));


    public static void main(String[] args) {
        new StreamsTweetsFilter().run();
    }


    public void run() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> inputTopic = streamsBuilder.stream(inputKafkaTopic);
        KStream<String, String> filteredStream = inputTopic.filter((key, tweet) -> doesContain(tweet));
        filteredStream.to(outputKafkaTopic);

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), KafkaStreamsConfig.getExactlyOnceProperties("twitter_filter"));

        kafkaStreams.start();
    }


    private boolean doesContain(String tweet) {
        try {
            String tweetText = JsonParser.parseString(tweet)
                                        .getAsJsonObject()
                                        .get("text")
                                        .getAsString()
                                        .toLowerCase();

            long unmatched = shouldBePresent.stream()
                                            .map(String::toLowerCase)
                                            .filter(keyword -> !tweetText.contains(keyword))
                                            .count();

            if (unmatched == 0L)
                return true;

        } catch (NullPointerException e) {
            logger.error("Error while processing tweet: " + tweet, e);
        }

        return false;
    }
}
