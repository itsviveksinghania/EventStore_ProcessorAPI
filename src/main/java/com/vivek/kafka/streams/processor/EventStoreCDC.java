package com.vivek.kafka.streams.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.fge.jsonpatch.JsonPatch;
import com.github.fge.jsonpatch.diff.JsonDiff;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
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
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "EventStoreCDC1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Replace with your Kafka bootstrap servers
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class.getName());
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

        TopicNameExtractor<String, JsonNode> topicExtractor = (key, value, recordContext) -> recordContext.topic()+System.getenv("EVENT_STORE_CDC_DIFF_TOPIC_SUFFIX");

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

    private static class EventStore_CDCProcessor implements Processor<String, JsonNode, String, JsonNode> {
        private ProcessorContext<String, JsonNode> context;
        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public void init(ProcessorContext<String, JsonNode> context) {
            this.context = context;
        }

        @Override
        public void process(Record<String, JsonNode> record) {
            String topic = null;
            Optional<RecordMetadata> recordMetadataOptional = context.recordMetadata();
            if (recordMetadataOptional.isPresent()) {
                topic = recordMetadataOptional.get().topic();
            } else {
                System.out.println("RecordMetadata is not present");
            }

            KeyValueStore<String, String> kvStore = context.getStateStore("EventStoreKVStateStore");

            String jsonString = null;
            try {
                jsonString = objectMapper.writeValueAsString(record.value()); // Convert JsonNode to String
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }

            Configuration conf = Configuration.builder()
                    .jsonProvider(new JacksonJsonProvider())
                    .options(Option.SUPPRESS_EXCEPTIONS)
                    .build();
            String valueJsonPath = "$."+System.getenv("EVENT_STORE_CDC_VALUE_JSON_PATH");
            Object payloadObject = JsonPath.using(conf).parse(jsonString).read(valueJsonPath); // Get the Payload Part of the Record.value
            String payloadJsonString = conf.jsonProvider().toJson(payloadObject);

            try {
                String storeJson = kvStore.get(topic + System.getenv("EVENT_STORE_CDC_KEY_VALUE_STORE_SUFFIX"));

                if (storeJson == null){

                    if (Objects.equals(System.getenv("EVENT_STORE_CDC_JSON_DIFF_ENABLE"), "1")){

                        JsonNode sourceNode = objectMapper.readTree("{}");
                        JsonNode targetNode = objectMapper.readTree(payloadJsonString);

                        // Compute the patch (the difference) between the source and target JSON
                        JsonPatch patch = JsonDiff.asJsonPatch(sourceNode, targetNode);

                        ObjectNode valueNode = objectMapper.createObjectNode();
                        valueNode.set("before", sourceNode);
                        valueNode.set("after", targetNode);
                        valueNode.set("diff", objectMapper.valueToTree(patch));
                        valueNode.set("timestamp", JsonNodeFactory.instance.numberNode(record.timestamp()));

                        context.forward(new Record<>(record.key(), valueNode, record.timestamp()));
                    }
                    else {

                        JsonNode sourceNode = objectMapper.readTree("{}");
                        JsonNode targetNode = objectMapper.readTree(payloadJsonString);

                        ObjectNode valueNode = objectMapper.createObjectNode();
                        valueNode.set("before", sourceNode);
                        valueNode.set("after", targetNode);
                        valueNode.set("timestamp", JsonNodeFactory.instance.numberNode(record.timestamp()));

                        context.forward(new Record<>(record.key(), valueNode, record.timestamp()));
                    }

                }
                else{

                    if (Objects.equals(System.getenv("EVENT_STORE_CDC_JSON_DIFF_ENABLE"), "1")){
                        JsonNode targetNode = objectMapper.readTree(payloadJsonString);
                        JsonNode sourceNode = objectMapper.readTree(storeJson);

                        // Compute the patch (the difference) between the source and target JSON
                        JsonPatch patch = JsonDiff.asJsonPatch(sourceNode, targetNode);

                        ObjectNode valueNode = objectMapper.createObjectNode();
                        valueNode.set("before", sourceNode);
                        valueNode.set("after", targetNode);
                        valueNode.set("diff", objectMapper.valueToTree(patch));
                        valueNode.set("timestamp", JsonNodeFactory.instance.numberNode(record.timestamp()));

                        context.forward(new Record<>(record.key(), valueNode, record.timestamp()));
                    }
                    else{
                        JsonNode targetNode = objectMapper.readTree(payloadJsonString);
                        JsonNode sourceNode = objectMapper.readTree(storeJson);

                        ObjectNode valueNode = objectMapper.createObjectNode();
                        valueNode.set("before", sourceNode);
                        valueNode.set("after", targetNode);
                        valueNode.set("timestamp", JsonNodeFactory.instance.numberNode(record.timestamp()));

                        context.forward(new Record<>(record.key(), valueNode, record.timestamp()));
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

    private static class ChangeLog_UpdateProcessor implements Processor<String, JsonNode, String, JsonNode>{
        private ProcessorContext<String, JsonNode> context;
        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public void init(ProcessorContext<String, JsonNode> context) {
            this.context = context;
        }

        @Override
        public void process(Record<String, JsonNode> record) {
            String topicName = null;
            Optional<RecordMetadata> recordMetadataOptional = context.recordMetadata();
            if (recordMetadataOptional.isPresent()) {
                topicName = recordMetadataOptional.get().topic();
            } else {
                System.out.println("RecordMetadata is not present");
            }
            String kvStoreKey = topicName + System.getenv("EVENT_STORE_CDC_KEY_VALUE_STORE_SUFFIX");

            KeyValueStore<String, String> kvStore = context.getStateStore("EventStoreKVStateStore");

            String jsonString = null;
            try {
                jsonString = objectMapper.writeValueAsString(record.value()); // Convert JsonNode to String
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }

            Configuration conf = Configuration.builder()
                    .jsonProvider(new JacksonJsonProvider())
                    .options(Option.SUPPRESS_EXCEPTIONS)
                    .build();
            Object afterObject = JsonPath.using(conf).parse(jsonString).read("$.after"); // Get the after, part of the Record.value
            String afterJsonString = conf.jsonProvider().toJson(afterObject);

            kvStore.put(kvStoreKey, afterJsonString);

            context.forward(new Record<>(record.key(), record.value(), record.timestamp()));
        }

        @Override
        public void close() {
            // No Custom Logic Required
        }
    }

    public static final class JsonSerializer implements Serializer<JsonNode> {
        private final ObjectMapper objectMapper = new ObjectMapper();
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            // Nothing to configure
        }

        @Override
        public byte[] serialize(String topic, JsonNode data) {
            if (data == null) {
                return null;
            }

            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new SerializationException("Error serializing JSON message", e);
            }
        }

        @Override
        public byte[] serialize(String topic, Headers headers, JsonNode data) {
            // handle headers here if necessary
            return serialize(topic, data);
        }

        @Override
        public void close() {
            // Nothing to close
        }
    }

    public static final class JsonDeserializer implements Deserializer<JsonNode> {
        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            // Nothing to configure
        }

        @Override
        public JsonNode deserialize(String s, byte[] data) {
            if (data == null) {
                return null;
            }

            try {
                return objectMapper.readTree(data);
            } catch (Exception e) {
                throw new SerializationException("Error deserializing JSON message", e);
            }
        }

        @Override
        public JsonNode deserialize(String topic, Headers headers, byte[] data) {
            // handle headers here if necessary
            return deserialize(topic, data);
        }

        @Override
        public void close() {
            // Nothing to close
        }
    }

    public static class JsonSerde implements Serde<JsonNode> {
        final JsonSerializer serializer;
        final JsonDeserializer deserializer;

        public JsonSerde() {
            this.serializer = new JsonSerializer();
            this.deserializer = new JsonDeserializer();
        }

        @Override
        public Serializer<JsonNode> serializer() {
            return serializer;
        }

        @Override
        public Deserializer<JsonNode> deserializer() {
            return deserializer;
        }
    }
}
