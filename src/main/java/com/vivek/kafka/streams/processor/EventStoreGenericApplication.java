package com.vivek.kafka.streams.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonpatch.JsonPatch;
import com.github.fge.jsonpatch.diff.JsonDiff;
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
import java.util.regex.Pattern;

public class EventStoreGenericApplication {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "EventStoreGenericApplication");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Replace with your Kafka bootstrap servers
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once");

        EventStoreGenericApplication EventStoreObj = new EventStoreGenericApplication();

        // Create a StreamsBuilder instance
        KafkaStreams kafkaStreams = new KafkaStreams(EventStoreObj.createTopology(), props);
        kafkaStreams.cleanUp();
        kafkaStreams.start();

        Topology describeTopology = EventStoreObj.createTopology();
        System.out.println(describeTopology.describe());

        // Add shutdown hook to gracefully close the KafkaStreams instance
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }

    public Topology createTopology(){

        Topology topology = new Topology();

        Pattern patternInput = Pattern.compile("poo_.*");

        TopicNameExtractor<String, String> topicExtractor = (key, value, recordContext) -> "_output_"+recordContext.topic();

        // Add Source processor
        topology.addSource("InputProcessor", patternInput)

                // Add Process processor
                .addProcessor("EventStore_GenericProcessor", EventStore_GenericProcessor::new, "InputProcessor")
                .addProcessor("ChangeLog_UpdateProcessor", ChangeLog_UpdateProcessor::new, "EventStore_GenericProcessor")

                // Add State Store
                .addStateStore(Stores.keyValueStoreBuilder(
                                Stores.persistentKeyValueStore("GenericJSONStateStore"),
                                Serdes.String(),
                                Serdes.String()),
                        "EventStore_GenericProcessor","ChangeLog_UpdateProcessor")

                // Add Sink processor
                .addSink("OutputProcessor", topicExtractor, "ChangeLog_UpdateProcessor");

        return topology;
    }

    private static class EventStore_GenericProcessor implements Processor<String, String, String, String> {
        private ProcessorContext<String, String> context;
        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public void init(ProcessorContext<String, String> context) {
            this.context = context;
        }

        @Override
        public void process(Record<String, String> record) {

            KeyValueStore<String, String> kvStore = context.getStateStore("GenericJSONStateStore");

            try {
                String storeJson = kvStore.get(record.key());

                if (storeJson == null){

                    JsonNode sourceNode = objectMapper.readTree("{}");
                    JsonNode targetNode = objectMapper.readTree(record.value());

                    // Compute the patch (the difference) between the source and target JSON
                    JsonPatch patch = JsonDiff.asJsonPatch(sourceNode, targetNode);

                    String valueString = String.format("before: %s, after: %s, diff: %s, timestamp: %s",
                            "null", record.value(), patch, record.timestamp());

                    context.forward(new Record<>(record.key(), valueString, record.timestamp()));
                }
                else{
                    JsonNode sourceNode = objectMapper.readTree(storeJson);
                    JsonNode targetNode = objectMapper.readTree(record.value());

                    // Compute the patch (the difference) between the source and target JSON
                    JsonPatch patch = JsonDiff.asJsonPatch(sourceNode, targetNode);

                    String valueString = String.format("before: %s, after: %s, diff: %s, timestamp: %s",
                            storeJson, record.value(), patch, record.timestamp());

                    context.forward(new Record<>(record.key(), valueString, record.timestamp()));
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

            KeyValueStore<String, String> kvStore = context.getStateStore("GenericJSONStateStore");

            String key = record.key();
            String valueString = record.value();

            String afterKeyword = "after: ";
            int startIdx = valueString.indexOf(afterKeyword);
            if (startIdx == -1) {
                throw new IllegalArgumentException("String does not contain 'after: '");
            }
            startIdx += afterKeyword.length(); // Move index to the start of the JSON string

            int braceCount = 0;
            int endIdx = -1;
            for (int i = startIdx; i < valueString.length(); i++) {
                if (valueString.charAt(i) == '{') {
                    braceCount++;
                } else if (valueString.charAt(i) == '}') {
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

            String afterValue = valueString.substring(startIdx, endIdx);

            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode;

            try {
                jsonNode = objectMapper.readTree(afterValue);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }

            kvStore.put(key, String.valueOf(jsonNode));

            context.forward(new Record<>(key, valueString, record.timestamp()));
        }

        @Override
        public void close() {
            // No Custom Logic Required
        }
    }

}
