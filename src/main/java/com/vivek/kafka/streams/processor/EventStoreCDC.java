package com.vivek.kafka.streams.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonpatch.JsonPatch;
import com.github.fge.jsonpatch.diff.JsonDiff;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EventStoreCDC {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "EventStoreCDC");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Replace with your Kafka bootstrap servers
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once");

        EventStoreCDC EventStoreObj = new EventStoreCDC();
        EventStoreObj.checkAndCreateTopic();

        // Create a StreamsBuilder instance
        KafkaStreams kafkaStreams = new KafkaStreams(EventStoreObj.createTopology(), props);
        kafkaStreams.cleanUp();
        kafkaStreams.start();

        Topology describeTopology = EventStoreObj.createTopology();
        System.out.println(describeTopology.describe());

        // Add shutdown hook to gracefully close the KafkaStreams instance
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }

    public void checkAndCreateTopic(){
        Properties AdminProps = new Properties();
        AdminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        try (AdminClient client = AdminClient.create(AdminProps)) {
            ListTopicsResult topics = client.listTopics();
            List<String> topicNames = new ArrayList<>();
            Pattern patternInput = Pattern.compile(System.getenv("EVENT_STORE_CDC_INPUT_TOPIC_PATTERN"));

            for (TopicListing topic : topics.listings().get()) {
                Matcher matcher = patternInput.matcher(topic.name());
                if (matcher.matches()) {
                    topicNames.add(topic.name());
                }
            }

            for (String topicName : topicNames) {
                String newTopicName = topicName + System.getenv("EVENT_STORE_CDC_DIFF_TOPIC_SUFFIX");
                try {
                    client.createTopics(
                            List.of(new NewTopic(newTopicName, 1, (short) 1))).all().get();
                } catch (ExecutionException e) {
                    if (!(e.getCause() instanceof TopicExistsException)) {
                        throw e;
                    }
                    // Topic already exists, which is fine.
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Topology createTopology(){

        Topology topology = new Topology();

        Pattern patternInput = Pattern.compile(System.getenv("EVENT_STORE_CDC_INPUT_TOPIC_PATTERN"));

        TopicNameExtractor<String, String> topicExtractor = (key, value, recordContext) -> recordContext.topic()+System.getenv("EVENT_STORE_CDC_DIFF_TOPIC_SUFFIX");

        Map<String, String> changelogConfig = new HashMap<>(); // For Logs

        // Add Source processor
        topology.addSource("InputProcessor", patternInput)

                // Add Process processor
                .addProcessor("EventStore_CDCProcessor", EventStore_CDCProcessor::new, "InputProcessor")
                .addProcessor("ChangeLog_UpdateProcessor", ChangeLog_UpdateProcessor::new, "EventStore_CDCProcessor")

                // Add State Store
                .addStateStore(Stores.keyValueStoreBuilder(
                                Stores.persistentKeyValueStore("EventStoreKVStateStore"),
                                Serdes.String(),
                                Serdes.String()).withLoggingEnabled(changelogConfig),
                        "EventStore_CDCProcessor","ChangeLog_UpdateProcessor")

                // Add Sink processor
                .addSink("OutputProcessor", topicExtractor, "ChangeLog_UpdateProcessor");

        return topology;
    }

    private static class EventStore_CDCProcessor implements Processor<String, String, String, String> {
        private ProcessorContext<String, String> context;
        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public void init(ProcessorContext<String, String> context) {
            this.context = context;
        }

        @Override
        public void process(Record<String, String> record) {
            String topic = null;

            Optional<RecordMetadata> recordMetadataOptional = context.recordMetadata();
            if (recordMetadataOptional.isPresent()) {
                topic = recordMetadataOptional.get().topic();
            } else {
                System.out.println("RecordMetadata is not present");
            }

            KeyValueStore<String, String> kvStore = context.getStateStore("EventStoreKVStateStore");

            try {
                String storeJson = kvStore.get(topic + System.getenv("EVENT_STORE_CDC_KEY_VALUE_STORE_SUFFIX"));

                if (storeJson == null){

                    if (Objects.equals(System.getenv("EVENT_STORE_CDC_JSON_DIFF_ENABLE"), "1")){
                        JsonNode sourceNode = objectMapper.readTree("{}");
                        JsonNode targetNode = objectMapper.readTree(record.value());

                        // Compute the patch (the difference) between the source and target JSON
                        JsonPatch patch = JsonDiff.asJsonPatch(sourceNode, targetNode);

                        String valueString = String.format("before: %s, after: %s, diff: %s, timestamp: %s",
                                "null", record.value(), patch, record.timestamp());

                        context.forward(new Record<>(record.key(), valueString, record.timestamp()));
                    }
                    else {
                        String valueString = String.format("before: %s, after: %s, timestamp: %s",
                                "null", record.value(), record.timestamp());

                        context.forward(new Record<>(record.key(), valueString, record.timestamp()));
                    }


                }
                else{

                    if (Objects.equals(System.getenv("EVENT_STORE_CDC_JSON_DIFF_ENABLE"), "1")){
                        JsonNode sourceNode = objectMapper.readTree(storeJson);
                        JsonNode targetNode = objectMapper.readTree(record.value());

                        // Compute the patch (the difference) between the source and target JSON
                        JsonPatch patch = JsonDiff.asJsonPatch(sourceNode, targetNode);

                        String valueString = String.format("before: %s, after: %s, diff: %s, timestamp: %s",
                                storeJson, record.value(), patch, record.timestamp());

                        context.forward(new Record<>(record.key(), valueString, record.timestamp()));
                    }
                    else{
                        String valueString = String.format("before: %s, after: %s, timestamp: %s",
                                storeJson, record.value(), record.timestamp());

                        context.forward(new Record<>(record.key(), valueString, record.timestamp()));
                    }

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

    private static class ChangeLog_UpdateProcessor implements Processor<String, String, String, String>{
        private ProcessorContext<String, String> context;

        @Override
        public void init(ProcessorContext<String, String> context) {
            this.context = context;
        }

        @Override
        public void process(Record<String, String> record) {
            String topicName = null;

            Optional<RecordMetadata> recordMetadataOptional = context.recordMetadata();
            if (recordMetadataOptional.isPresent()) {
                topicName = recordMetadataOptional.get().topic();
            } else {
                System.out.println("RecordMetadata is not present");
            }

            KeyValueStore<String, String> kvStore = context.getStateStore("EventStoreKVStateStore");

            String kvStoreKey = topicName + System.getenv("EVENT_STORE_CDC_KEY_VALUE_STORE_SUFFIX");
            String RecordValue = record.value();

            // To get the JSON associated with after
            String afterKeyword = "after: ";
            int startIdx = RecordValue.indexOf(afterKeyword);
            if (startIdx == -1) {
                throw new IllegalArgumentException("String does not contain 'after: '");
            }
            startIdx += afterKeyword.length(); // Move index to the start of the JSON string

            int braceCount = 0;
            int endIdx = -1;
            for (int i = startIdx; i < RecordValue.length(); i++) {
                if (RecordValue.charAt(i) == '{') {
                    braceCount++;
                } else if (RecordValue.charAt(i) == '}') {
                    braceCount--;
                    if (braceCount == 0) {
                        endIdx = i + 1; // Move index to the end of the JSON string
                        break;
                    }
                }
            }

            if (endIdx == -1) {
                throw new IllegalArgumentException("JSON string is not properly closed");
            }

            String afterValue = RecordValue.substring(startIdx, endIdx);

            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode;

            try {
                jsonNode = objectMapper.readTree(afterValue);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }

            // Updating KVStore
            kvStore.put(kvStoreKey, String.valueOf(jsonNode));

            context.forward(new Record<>(record.key(), record.value(), record.timestamp()));
        }

        @Override
        public void close() {
            // No Custom Logic Required
        }
    }

}
