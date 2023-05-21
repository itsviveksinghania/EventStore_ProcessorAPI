package com.vivek.kafka.streams.processor;


import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class EventStoreAppTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, String> outputTopic;

    @Before
    public void setUp() {
        // Configure the Kafka Streams properties
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "eventStore-test");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Create the topology and initialize the test driver
        EventStore eventStore = new EventStore();
        Topology topology = eventStore.createTopology();
        testDriver = new TopologyTestDriver(topology, properties);

        // Create the test input and output topics
        inputTopic = testDriver.createInputTopic("topic1", Serdes.String().serializer(), Serdes.String().serializer());
        outputTopic = testDriver.createOutputTopic("outputTopic", Serdes.String().deserializer(), Serdes.String().deserializer());
    }

    @After
    public void tearDown() {
        // Close the test driver
        testDriver.close();
    }

    @Test
    public void testEventProcessing() {
        // Send a test input record to the input topic
        inputTopic.pipeInput("key1", "{\"key\":\"key1\",\"type\":\"type1\",\"body\":\"body1\",\"version\":1}");

        // Read the output record from the output topic
        KeyValueStore<String, EventStore.StoreEvent> store = testDriver.getKeyValueStore("store1");
        EventStore.StoreEvent event = store.get("key1");

        // Verify the processed event
        assertEquals("key1", event.after.key);
        assertEquals("type1", event.after.type);
        assertEquals("body1", event.after.body);
        assertEquals(1, event.after.version);
    }

    @Test
    public void testEventWithMissingKey() {
        // Send a test input record without the "key" field
        inputTopic.pipeInput("{\"type\":\"type1\",\"body\":\"body1\",\"version\":1}");

        // Read the output record from the output topic
        KeyValueStore<String, EventStore.StoreEvent> store = testDriver.getKeyValueStore("store1");
        EventStore.StoreEvent event = store.get("key1");

        // Verify that the event was not stored
        assertNull(event);
    }
}
