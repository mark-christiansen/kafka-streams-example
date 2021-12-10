package com.machrist.kafka.streams;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;

public class TestStream {

    private final TopologyTestDriver testDriver;
    private final TestInputTopic<String, String> inputTopic;
    private final TestOutputTopic<String, GenericRecord> successOutputTopic;
    private final KafkaProducer<String, String> kafkaProducer;

    public TestStream(TopologyTestDriver testDriver,
                      TestInputTopic<String, String> inputTopic,
                      TestOutputTopic<String, GenericRecord> successOutputTopic,
                      KafkaProducer<String, String> kafkaProducer) {
        this.testDriver = testDriver;
        this.inputTopic = inputTopic;
        this.successOutputTopic = successOutputTopic;
        this.kafkaProducer = kafkaProducer;
    }

    public TestInputTopic<String, String> getInputTopic() {
        return inputTopic;
    }

    public TestOutputTopic<String, GenericRecord> getSuccessOutputTopic() {
        return successOutputTopic;
    }

    public KafkaProducer<String, String> getKafkaProducer() {
        return kafkaProducer;
    }

    public void close() {
        this.testDriver.close();
    }
}
