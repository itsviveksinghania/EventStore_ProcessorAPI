package com.vivek.kafka.streams.processor;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static junit.framework.TestCase.assertEquals;

public class EventStoreAppTest {

    private TopologyTestDriver testDriver;

    @Before
    public void setUp() {
        // Properties
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "eventStore");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        EventStoreApp storeApp2 = new EventStoreApp();
        Topology topology = storeApp2.createTopology();

        testDriver = new TopologyTestDriver(topology, properties);
    }

    @After
    public void tearDown() {
        testDriver.close();
    }

    @Test
    public void testEventStoreApp2() {
        // Send input events to input topics
        TestInputTopic<String, String> inputTopic1 = testDriver.createInputTopic("topic1",
                Serdes.String().serializer(), Serdes.String().serializer());
        inputTopic1.pipeInput("key1", "{\"key\": \"key1\", \"type\": \"type1\", \"body\": \"body1\", \"version\": 1}");

        TestInputTopic<String, String> inputTopic2 = testDriver.createInputTopic("topic2",
                Serdes.String().serializer(), Serdes.String().serializer());
        inputTopic2.pipeInput("key2", "{\"key\": \"key2\", \"type\": \"type2\", \"body\": \"body2\", \"version\": 2}");

        // Read output from output topics
        TestOutputTopic<String, String> outputTopic1 = testDriver.createOutputTopic("store1",
                Serdes.String().deserializer(), Serdes.String().deserializer());
        TestOutputTopic<String, String> outputTopic2 = testDriver.createOutputTopic("store2",
                Serdes.String().deserializer(), Serdes.String().deserializer());

        // Verify the output records
        assertEquals(outputTopic1.readRecord(), (new TestRecord<>("key1", "{\"before\":null,\"after\":\"{\\\"key\\\":\\\"key1\\\",\\\"type\\\":\\\"type1\\\",\\\"body\\\":\\\"body1\\\",\\\"version\\\":1}\",\"timestamp\":0}")));
        assertEquals(outputTopic2.readRecord(), (new TestRecord<>("key2", "{\"before\":null,\"after\":\"{\\\"key\\\":\\\"key2\\\",\\\"type\\\":\\\"type2\\\",\\\"body\\\":\\\"body2\\\",\\\"version\\\":2}\",\"timestamp\":0}")));
    }
}
