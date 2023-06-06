package com.vivek.kafka.streams.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EventStoreVersionApplication {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "EventStoreVersionApplication12");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Replace with your Kafka bootstrap servers
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once");

        EventStoreVersionApplication multipleTopic = new EventStoreVersionApplication();

        // Create a StreamsBuilder instance
        KafkaStreams kafkaStreams = new KafkaStreams(multipleTopic.createTopology(), props);
        kafkaStreams.cleanUp();
        kafkaStreams.start();

        Topology describeTopology = multipleTopic.createTopology();
        System.out.println(describeTopology.describe());

        // Add shutdown hook to gracefully close the KafkaStreams instance
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }

    public Topology createTopology(){

        Topology topology = new Topology();
        Pattern patternInput = Pattern.compile("_input_foo_.*");

        TopicNameExtractor<String, String> topicExtractor = (key, value, recordContext) -> "_output_"+recordContext.topic();

        // Add Source processor
        topology.addSource("InputProcessor", patternInput)

                // Add Process processor
                .addProcessor("EventStore_VersionProcessor", EventStore_VersionProcessor::new, "InputProcessor")
                .addProcessor("ChangeVersion_UpdateProcessor", ChangeVersion_StateStore::new, "EventStore_VersionProcessor")

                // Add State Store
                .addStateStore(Stores.keyValueStoreBuilder(
                                Stores.persistentKeyValueStore("VersionStateStore"),
                                Serdes.String(),
                                Serdes.Integer()),
                        "EventStore_VersionProcessor","ChangeVersion_UpdateProcessor")

                // Add Sink processor
                .addSink("OutputProcessor", topicExtractor, "ChangeVersion_UpdateProcessor");

        return topology;
    }

    private static class EventStore_VersionProcessor implements Processor<String, String, String, String> {
        private ProcessorContext<String, String> context;
        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public void init(ProcessorContext<String, String> context) {
            this.context = context;
        }

        @Override
        public void process(Record<String, String> record) {

            KeyValueStore<String, Integer> kvStore = context.getStateStore("VersionStateStore");

            try {
                JsonNode jsonObject = objectMapper.readTree(record.value());
                String key = jsonObject.get("key").asText();
                int version = jsonObject.get("version").asInt();

                Integer storeVersion = kvStore.get(key);

                if (storeVersion == null){
                    context.forward(new Record<>(key, "before: "+ null + ",  after: "+ version + ",  timestamp: "+record.timestamp(), record.timestamp()));
                }
                else{
                    context.forward(new Record<>(key, "before: "+ storeVersion + ",  after: "+ version + ",  timestamp: "+record.timestamp(), record.timestamp()));
                }

            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() {
            // No custom close logic needed
        }
    }

    private static class ChangeVersion_StateStore implements Processor<String, String, String, String>{
        private ProcessorContext<String, String> context;

        @Override
        public void init(ProcessorContext<String, String> context) {
            this.context = context;
        }

        @Override
        public void process(Record<String, String> record) {

            KeyValueStore<String, Integer> kvStore = context.getStateStore("VersionStateStore");

            String key = record.key();
            String value = record.value();

            Pattern pattern = Pattern.compile("after:\\s*(\\d+)");
            Matcher matcher = pattern.matcher(value);
            if (matcher.find()) {
                kvStore.put(key,Integer.parseInt(matcher.group(1)));
            }
            context.forward(new Record<>(key, value, record.timestamp()));
        }

        @Override
        public void close() {
            // No Custom Logic Required
        }
    }

}
