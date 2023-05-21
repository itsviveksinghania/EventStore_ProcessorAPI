package com.vivek.kafka.streams.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

public class EventStore {
    public static void main(String[] args) {
        // Properties
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "eventStore");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        EventStore storeApp = new EventStore();
        KafkaStreams streams = new KafkaStreams(storeApp.createTopology(), properties);
        streams.cleanUp();
        streams.start();

        Topology describeTopology = storeApp.createTopology();
        System.out.println(describeTopology.describe());

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public Topology createTopology() {
        Topology topology = new Topology();

        // Map of topic name to key-value store name
        Map<String, String> topicStoreMap = new HashMap<>();
        topicStoreMap.put("topic1", "store1");
        topicStoreMap.put("topic2", "store2");
        // Add more topics and corresponding stores as needed

        // Create a map to hold the store builders
        Map<String, StoreBuilder<KeyValueStore<String, EventStore.StoreEvent>>> storeBuilders = new HashMap<>();

        // Add sources and processors for each topic
        for (String topic : topicStoreMap.keySet()) {
            String storeName = topicStoreMap.get(topic);

            // Check if the store builder already exists
            if (!storeBuilders.containsKey(storeName)) {
                storeBuilders.put(storeName, createKeyValueStoreBuilder(storeName));
            }

            // Create a new instance of the ProcessorSupplier for each topic
            StoreProcessorSupplier processorSupplier =
                    new StoreProcessorSupplier(storeName, new JsonPathKeyExtractor("/key"));

            // Add the source processor for the topic
            topology.addSource("Source-" + topic, String.valueOf(Consumed.with(Serdes.String(), Serdes.String())), topic);

            // Add the processor and sink for the topic
            topology.addProcessor("Processor-" + storeName, processorSupplier, "Source-" + topic)
                    .addSink("Sink-" + storeName, "outputTopic", "Processor-" + storeName);
        }
        return topology;
    }

    private static class StoreEventSerializer implements Serializer<StoreEvent> {
        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public byte[] serialize(String topic, StoreEvent data) {
            try {
                return objectMapper.writeValueAsString(data).getBytes(StandardCharsets.UTF_8);
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Error serializing StoreEvent", e);
            }
        }
    }

    private static class StoreEventDeserializer implements Deserializer<StoreEvent> {
        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public StoreEvent deserialize(String topic, byte[] data) {
            try {
                return objectMapper.readValue(new String(data, StandardCharsets.UTF_8), StoreEvent.class);
            } catch (IOException e) {
                throw new RuntimeException("Error deserializing StoreEvent", e);
            }
        }
    }

    private static StoreBuilder<KeyValueStore<String, StoreEvent>> createKeyValueStoreBuilder(String storeName) {

        Serializer<String> keySerializer = Serdes.String().serializer();
        Deserializer<String> keyDeserializer = Serdes.String().deserializer();
        Serializer<StoreEvent> valueSerializer = new StoreEventSerializer();
        Deserializer<StoreEvent> valueDeserializer = new StoreEventDeserializer();

        return Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(storeName),
                Serdes.serdeFrom(keySerializer, keyDeserializer),
                Serdes.serdeFrom(valueSerializer, valueDeserializer)
        );
    }

    public static class StoreProcessorSupplier implements ProcessorSupplier<String, String, String, String> {

        private final String storeName;
        private final KeyExtractor keyExtractor;

        public StoreProcessorSupplier(String storeName, KeyExtractor keyExtractor) {
            this.storeName = storeName;
            this.keyExtractor = keyExtractor;
        }
        @Override
        public Processor<String, String, String, String> get() {
            return new StoreProcessor(storeName, keyExtractor);
        }

        @Override
        public Set<StoreBuilder<?>> stores() {
            final StoreBuilder<KeyValueStore<String, StoreEvent>> eventStoreBuilder = createKeyValueStoreBuilder(storeName);
            return Collections.singleton(eventStoreBuilder);
        }
    }

    public static class StoreProcessor implements Processor<String, String, String, String>{

        private final String storeName;
        private KeyValueStore<String, StoreEvent> kvStore;
        private final KeyExtractor keyExtractor;

        public StoreProcessor(String storeName, KeyExtractor keyExtractor) {
            this.storeName = storeName;
            this.keyExtractor = keyExtractor;
        }

        @Override
        public void init(ProcessorContext<String, String> context) {
            kvStore = context.getStateStore(storeName);

            context.schedule(Duration.ofSeconds(1), PunctuationType.STREAM_TIME, timestamp -> {
                try (final KeyValueIterator<String, StoreEvent> iterator = kvStore.all()) {
                    while (iterator.hasNext()) {
                        final KeyValue<String, StoreEvent> entry = iterator.next();
                        context.forward(new Record<>(entry.key, entry.value.toString(), timestamp));
                    }
                }
            });
        }

        @Override
        public void process(Record<String, String> record) {

            String jsonRecord = record.value();
            Event event = null;
            try {
                ObjectMapper objectMapper = new ObjectMapper();
                JsonNode jsonNode = objectMapper.readTree(jsonRecord);

                String key = jsonNode.get("key").asText();
                String type = jsonNode.get("type").asText();
                String body = jsonNode.get("body").asText();
                int version = jsonNode.get("version").asInt();

                event = new Event();
                event.key = key;
                event.type = type;
                event.body = body;
                event.version = version;

            } catch (Exception e) {
                e.printStackTrace();
            }

            String key = keyExtractor.extractKey(record.value());
            if (key == null) {
                throw new StreamsException("Key extraction failed for record: " + record);
            }

            StoreEvent oldEvent = kvStore.get(key);


            if (oldEvent == null || oldEvent.after.version == Objects.requireNonNull(event).version - 1) {
                StoreEvent newEvent = new StoreEvent();
                if (oldEvent != null) {
                    newEvent.before = oldEvent.after;
                }
                newEvent.after = event;
                newEvent.timestamp = record.timestamp();
                kvStore.put(key, newEvent);
            }
        }

        @Override
        public void close() {
            // No custom close logic needed
        }
    }

    public interface KeyExtractor{
        String extractKey(String value);
    }

    public static class JsonPathKeyExtractor implements KeyExtractor{

        private final String jsonPath;

        public JsonPathKeyExtractor(String jsonPath) {
            this.jsonPath = jsonPath;
        }

        @Override
        public String extractKey(String value) {
            try {
                JsonNode jsonNode = new ObjectMapper().readTree(value);
                return jsonNode.at(jsonPath).asText();
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }
    }

    public static class StoreEvent {
        public Event before;
        public Event after;
        public long timestamp;
    }

    public static class Event {
        public String key;
        public String type;
        public String body;
        public int version;

    }
}
