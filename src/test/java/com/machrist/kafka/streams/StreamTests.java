package com.machrist.kafka.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@DisplayName("Stream topology tests")
public class StreamTests {

    private static final String APPLICATION_ID = "test-application-id";
    private static final String SCHEMA_REGISTRY_URL = "mock://test-schema-registry";

    private final TestStreamHelper testStreamHelper = new TestStreamHelper();
    private TestStream stream;

    @BeforeEach
    public void setup() {
        Properties appProps = testStreamHelper.getApplicationProperties();
        this.stream = testStreamHelper.createTestStream(appProps, APPLICATION_ID, SCHEMA_REGISTRY_URL);
    }

    @AfterEach
    public void cleanup() {
        this.stream.close();
    }

    @Test
    @DisplayName("Stream valid bae64 encoded JSON string with customer, address1 and address2 data")
    public void testStreamValidFullCustomer() throws JsonProcessingException {

        TestInputTopic<String, String> inputTopic = stream.getInputTopic();
        TestOutputTopic<String, GenericRecord> successOutputTopic = stream.getSuccessOutputTopic();
        KafkaProducer<String, String> kafkaProducer = stream.getKafkaProducer();
        TestDataHelper dataHelper = new TestDataHelper();

        KeyValue<String, String> json = dataHelper.createFullCustomerEncodedJson();
        inputTopic.pipeInput(json.key, json.value);

        // verify the producer send wasn't called - only called if exceptions occur
        verify(kafkaProducer, times(0));

        // should have received three records - customer, address1 nd address2
        assertFalse(successOutputTopic.isEmpty(), "success topic is empty");
        TestRecord<String, GenericRecord> customer = successOutputTopic.readRecord();
        assertFalse(successOutputTopic.isEmpty(), "success topic is empty");
        TestRecord<String, GenericRecord> address1 = successOutputTopic.readRecord();
        assertFalse(successOutputTopic.isEmpty(), "success topic is empty");
        TestRecord<String, GenericRecord> address2 = successOutputTopic.readRecord();

        dataHelper.assertCustomerEqual(json.value, customer.getValue(), address1.getValue(), address2.getValue());
    }

    @Test
    @DisplayName("Stream valid bae64 encoded JSON string with customer but no address data")
    public void testStreamValidCustomerWithNoAddresses() throws JsonProcessingException {

        TestInputTopic<String, String> inputTopic = stream.getInputTopic();
        TestOutputTopic<String, GenericRecord> successOutputTopic = stream.getSuccessOutputTopic();
        KafkaProducer<String, String> kafkaProducer = stream.getKafkaProducer();
        TestDataHelper dataHelper = new TestDataHelper();

        KeyValue<String, String> json = dataHelper.createCustomerEncodedJson();
        inputTopic.pipeInput(json.key, json.value);

        // verify the producer send wasn't called - only called if exceptions occur
        verify(kafkaProducer, times(0));

        // should have received three records - customer, address1 nd address2
        assertFalse(successOutputTopic.isEmpty(), "success topic is empty");
        TestRecord<String, GenericRecord> customer = successOutputTopic.readRecord();
        dataHelper.assertCustomerEqual(json.value, customer.getValue(), null, null);
        assertTrue(successOutputTopic.isEmpty(), "success topic is not empty");
    }

    @Test
    @DisplayName("Stream invalid bae64 string")
    public void testStreamInvalidBase64() {

        TestInputTopic<String, String> inputTopic = stream.getInputTopic();
        TestOutputTopic<String, GenericRecord> successOutputTopic = stream.getSuccessOutputTopic();

        KafkaProducer<String, String> kafkaProducer = stream.getKafkaProducer();
        final ArgumentCaptor<ProducerRecord<String, String>> kafkaProducerCaptor = ArgumentCaptor.forClass(ProducerRecord.class);

        String key = "1";
        String value = "!@#$";
        inputTopic.pipeInput(key, value);

        // verify the producer send was called - only called if exceptions occur
        verify(kafkaProducer).send(kafkaProducerCaptor.capture());
        ProducerRecord<String, String> errorRecord = kafkaProducerCaptor.getValue();
        assertNotNull(errorRecord);
        assertEquals(key, errorRecord.key());
        assertEquals(value, errorRecord.value());

        // should have received no records
        assertTrue(successOutputTopic.isEmpty(), "success topic is not empty");
    }

    @Test
    @DisplayName("Stream invalid JSON string")
    public void testStreamInvalidJson() {

        TestInputTopic<String, String> inputTopic = stream.getInputTopic();
        TestOutputTopic<String, GenericRecord> successOutputTopic = stream.getSuccessOutputTopic();

        KafkaProducer<String, String> kafkaProducer = stream.getKafkaProducer();
        final ArgumentCaptor<ProducerRecord<String, String>> kafkaProducerCaptor = ArgumentCaptor.forClass(ProducerRecord.class);

        String key = "1";
        String value = "123ABC";
        inputTopic.pipeInput(key, value);

        // verify the producer send was called - only called if exceptions occur
        verify(kafkaProducer).send(kafkaProducerCaptor.capture());
        ProducerRecord<String, String> errorRecord = kafkaProducerCaptor.getValue();
        assertNotNull(errorRecord);
        assertEquals(key, errorRecord.key());
        assertEquals(value, errorRecord.value());

        // should have received no records
        assertTrue(successOutputTopic.isEmpty(), "success topic is not empty");
    }
}
