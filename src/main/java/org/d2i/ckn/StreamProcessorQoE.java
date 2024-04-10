// Import necessary packages and classes for Kafka Streams processing, logging, and configuration management
package org.d2i.ckn;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
import org.d2i.ckn.model.JsonSerde;
import org.d2i.ckn.model.qoe.AverageAggregator;
import org.d2i.ckn.model.qoe.CountSumAggregator;
import org.d2i.ckn.model.qoe.EventTimeExtractor;
import org.d2i.ckn.model.qoe.InferenceEvent;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Properties;

@Slf4j // Lombok annotation to automatically create a logger for this class
public class StreamProcessorQoE {
    // Define static variables to hold configuration parameters
    private static String inputTopic;
    private static String outputTopic;
    private static String countSumStore;
    private static String groupId;
    private static String bootstrapServers;
    private static String processorClientId;
    private static long timeWindowSize;
    private static int windowGracePeriod;

    // Main method to set up and start the Kafka Streams application
    public static void main(String[] args) {
        // Load configurations from config.properties file
        try (InputStream input = StreamProcessorQoE.class.getClassLoader().getResourceAsStream("config.properties")) {
            Properties config = new Properties();
            if (input == null) {
                log.error("The configuration file could not be found!");
                return;
            }
            config.load(input);

            // Assign configurations to static variables
            inputTopic = config.getProperty("stream.input.topic");
            outputTopic = config.getProperty("stream.output.topic");
            countSumStore = config.getProperty("stream.aggr.store");
            groupId = config.getProperty("stream.group.id");
            bootstrapServers = config.getProperty("stream.kafka.servers");
            processorClientId = config.getProperty("stream.kafka.clientId");
            timeWindowSize = Long.parseLong(config.getProperty("stream.kafka.windowSize"));
            windowGracePeriod = Integer.parseInt(config.getProperty("stream.kafka.window.gracePeriod"));

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // Set up properties for Kafka Streams
        Properties streamsProps = getProperties();

        // Build the Kafka Streams processing topology
        StreamsBuilder builder = getWindowedAggregationBuilder();

        // Initialize and start the Kafka Streams application
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsProps);
        kafkaStreams.start();
    }

    // Method to create and return a StreamsBuilder configured for windowed aggregation
    private static StreamsBuilder getWindowedAggregationBuilder() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // Define SerDes for custom object serialization and deserialization
        Serde<InferenceEvent> inferenceEventSerde = new JsonSerde<>(InferenceEvent.class);
        Serde<CountSumAggregator> countSumAggregatorSerde = new JsonSerde<>(CountSumAggregator.class);
        Serde<AverageAggregator> averageAggregatorSerde = new JsonSerde<>(AverageAggregator.class);

        // Define the stream processing logic
        KStream<String, InferenceEvent> inferenceEventKStream = streamsBuilder.stream(inputTopic,
                Consumed.with(Serdes.String(), inferenceEventSerde)
                        .withTimestampExtractor(new EventTimeExtractor()));

        // Group the stream by key, window the grouped stream, and aggregate values
        inferenceEventKStream.groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofSeconds(timeWindowSize)).grace(Duration.ofSeconds(windowGracePeriod)))
                .aggregate(CountSumAggregator::new,
                        (key, value, aggregate) -> aggregate.process(value),
                        Materialized.<String, CountSumAggregator, WindowStore<Bytes, byte[]>>as(countSumStore)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(countSumAggregatorSerde)
                )
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream() // Convert to a stream for further processing
                .map((Windowed<String> winKey, CountSumAggregator value) -> {
                    return new KeyValue<>(winKey.key(), process_average(value));
                })
                .peek((key, value) -> log.info("Outgoing record - key " + key + " value " + value))
                .to(outputTopic, Produced.with(Serdes.String(), averageAggregatorSerde));

        return streamsBuilder;
    }

    // Helper method to calculate averages from aggregated values
    private static AverageAggregator process_average(CountSumAggregator value) {
        // Calculate averages from CountSumAggregator
        long count = value.getCount();
        float avg_req_accuracy = value.getAccuracy_total() / count;
        float avg_req_delay = value.getDelay_total() / count;
        float avg_total_qoe = value.getQoe_total_sum() / count;
        float avg_qoe_delay = value.getQoe_delay_total() / count;
        float avg_qoe_acc = value.getQoe_acc_total() / count;
        float avg_pred_acc = value.getPred_acc_total() / count;
        float avg_compute_time = value.getCompute_time_total() / count;
        // Return a new AverageAggregator object with calculated averages
        return new AverageAggregator(avg_req_accuracy, avg_req_delay, count, avg_total_qoe, avg_qoe_delay, avg_qoe_acc, avg_pred_acc, avg_compute_time, value.getClient_id(), value.getService_id(), value.getServer_id(), value.getModel(), System.currentTimeMillis());
    }

    // Example method showing a simpler stream processing topology (unused in main logic)
    private static StreamsBuilder getEdgeStreamsBuilder() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        Serde<InferenceEvent> inferenceEvents = new JsonSerde<>(InferenceEvent.class);

        KStream<String, Long> inferenceEventKStream = streamsBuilder.stream(inputTopic,
                        Consumed.with(Serdes.String(), inferenceEvents))
                .groupByKey()
                .count()
                .toStream()
                .peek((key, value) -> System.out.println("Outgoing record - key " + key + " value " + value));

        // Simplified stream processing logic for counting events...
        return streamsBuilder;
    }

    // Method to configure basic Kafka Streams properties
    private static Properties getProperties() {
        Properties configuration = new Properties();
        configuration.put(StreamsConfig.APPLICATION_ID_CONFIG, groupId);
        configuration.put(StreamsConfig.CLIENT_ID_CONFIG, processorClientId);
        configuration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configuration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        configuration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        configuration.setProperty(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, String.valueOf(1 * 1000));
        return configuration;
    }
}
