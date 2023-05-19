package com.vivek.kafka.streams.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsException;
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
import java.time.Duration;
import java.util.*;

public class EventStoreApp {
    public static void main(String[] args) {
        // Properties
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "eventStore");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        EventStoreApp storeApp = new EventStoreApp();
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
        Map<String, StoreBuilder<KeyValueStore<String, StoreEvent>>> storeBuilders = new HashMap<>();

        // Add sources and processors for each topic
        for (String topic : topicStoreMap.keySet()) {
            String storeName = topicStoreMap.get(topic);

            // Check if the store builder already exists
            if (!storeBuilders.containsKey(storeName)) {
                storeBuilders.put(storeName, createKeyValueStoreBuilder(storeName));
            }

            topology.addSource(topic, topic)
                    .addProcessor(storeName, new StoreProcessorSupplier(storeName, new JsonPathKeyExtractor("$.key")), topic)
                    .addStateStore(storeBuilders.get(storeName), storeName)
                    .addSink(storeName + "Sink", storeName, "outputTopic");
        }

        return topology;

    }

    private static StoreBuilder<KeyValueStore<String, StoreEvent>> createKeyValueStoreBuilder(String storeName) {
        return Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(storeName),
                Serdes.String(),
                new SpecificAvroSerde<>()
        );
    }

    public static class StoreProcessorSupplier implements ProcessorSupplier<String, Event, String, StoreEvent> {

        private final String storeName;
        private final KeyExtractor keyExtractor;

        public StoreProcessorSupplier(String storeName, KeyExtractor keyExtractor) {
            this.storeName = storeName;
            this.keyExtractor = keyExtractor;
        }

        @Override
        public Processor<String, Event, String, StoreEvent> get() {
            return new StoreProcessor(storeName, keyExtractor);
        }

        @Override
        public Set<StoreBuilder<?>> stores() {
            final StoreBuilder<KeyValueStore<String, StoreEvent>> eventStoreBuilder = createKeyValueStoreBuilder(storeName);
            return Collections.singleton(eventStoreBuilder);
        }
    }

    public static class StoreProcessor implements Processor<String, Event, String, StoreEvent> {

        private final String storeName;
        private KeyValueStore<String, StoreEvent> kvStore;
        private final KeyExtractor keyExtractor;

        public StoreProcessor(String storeName, KeyExtractor keyExtractor) {
            this.storeName = storeName;
            this.keyExtractor = keyExtractor;
        }

        @Override
        public void init(ProcessorContext<String, StoreEvent> context) {
            kvStore = context.getStateStore(storeName);

            context.schedule(Duration.ofSeconds(1), PunctuationType.STREAM_TIME, timestamp -> {
                try (final KeyValueIterator<String, StoreEvent> iter = kvStore.all()) {
                    while (iter.hasNext()) {
                        final KeyValue<String, StoreEvent> entry = iter.next();
                        context.forward(new Record<>(entry.key, entry.value, timestamp));
                    }
                }
            });
        }

        @Override
        public void process(Record<String, Event> record) {
            String key = keyExtractor.extractKey(record.value().toString());
            if (key == null) {
                throw new StreamsException("Key extraction failed for record: " + record);
            }

            StoreEvent oldEvent = kvStore.get(key);

            if (oldEvent == null || oldEvent.after.version == record.timestamp() - 1) {
                StoreEvent newEvent = new StoreEvent();
                if (oldEvent != null) {
                    newEvent.before = (oldEvent.after != null ? oldEvent.after : null);
                }
                newEvent.after = record.value();
                newEvent.timestamp = record.timestamp();
                kvStore.put(key, newEvent);
            }

        }

        @Override
        public void close() {
            // No custom close logic needed
        }
    }

    public interface KeyExtractor {
        String extractKey(String value);
    }

    public static class JsonPathKeyExtractor implements KeyExtractor {
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

    public static class StoreEvent implements SpecificRecord {
        public Event before;
        public Event after;
        public long timestamp;

        @Override
        public void put(int i, Object v) {
        }

        @Override
        public Object get(int i) {
            return null;
        }

        @Override
        public Schema getSchema() {
            return null;
        }
    }

    public static class Event {
        public String key;
        public String type;
        public String body;
        public int version;

    }
}
