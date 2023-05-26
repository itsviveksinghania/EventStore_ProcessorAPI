package com.vivek.kafka.streams.processor;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

import java.util.Properties;
import java.util.regex.Pattern;

public class MultipleTopic {
    public Topology createTopology(){
        Topology topology = new Topology();
        Pattern pattern = Pattern.compile("foo_.*");

        // Add source processor
        topology.addSource("SourceProcessor", pattern)
                .addProcessor("FooProcessor", FooProcessor::new, "SourceProcessor")
                .addSink("SinkProcessor", "output-topic", "FooProcessor");

        return topology;
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-application2");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Replace with your Kafka bootstrap servers
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        MultipleTopic multipleTopic = new MultipleTopic();

        // Create a StreamsBuilder instance
        KafkaStreams kafkaStreams = new KafkaStreams(multipleTopic.createTopology(), props);
        kafkaStreams.cleanUp();
        kafkaStreams.start();

        Topology describeTopology = multipleTopic.createTopology();
        System.out.println(describeTopology.describe());

        // Add shutdown hook to gracefully close the KafkaStreams instance
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }

    private static class FooProcessor implements Processor<String, String, String, String> {

        @Override
        public void init(ProcessorContext<String, String> context) {
            Processor.super.init(context);
        }

        @Override
        public void process(Record<String, String> record) {
            System.out.println("Received message: key = " + record.key() + ", value = " + record.value());
        }

        @Override
        public void close() {

        }
    }
}
