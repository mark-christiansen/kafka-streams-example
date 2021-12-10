package com.machrist.kafka.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@DisplayName("Stream topology tests")
public class StreamTests {

    private static final Logger log = LoggerFactory.getLogger(StreamTests.class);
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
        TestOutputTopic<String, GenericRecord> customerOutputTopic = stream.getCustomerOutputTopic();
        TestOutputTopic<String, GenericRecord> addressOutputTopic = stream.getAddressOutputTopic();
        KafkaProducer<String, String> kafkaProducer = stream.getKafkaProducer();
        TestDataHelper dataHelper = new TestDataHelper();

        KeyValue<String, String> json = dataHelper.createFullCustomerEncodedJson();
        inputTopic.pipeInput(json.key, json.value);

        // verify the producer send wasn't called - only called if exceptions occur
        verify(kafkaProducer, times(0)).send(any());

        // should have received three records - customer, address1 and address2
        assertFalse(customerOutputTopic.isEmpty(), "customer topic is empty");
        TestRecord<String, GenericRecord> customer = customerOutputTopic.readRecord();
        assertTrue(customerOutputTopic.isEmpty(), "customer topic is not empty");
        assertFalse(addressOutputTopic.isEmpty(), "address topic is empty");
        TestRecord<String, GenericRecord> address1 = addressOutputTopic.readRecord();
        assertFalse(addressOutputTopic.isEmpty(), "address topic is empty");
        TestRecord<String, GenericRecord> address2 = addressOutputTopic.readRecord();

        dataHelper.assertCustomerEqual(json.value, customer.getValue(), address1.getValue(), address2.getValue());
    }

    @Test
    @DisplayName("Stream valid bae64 encoded JSON string with customer but no address data")
    public void testStreamValidCustomerWithNoAddresses() throws JsonProcessingException {

        TestInputTopic<String, String> inputTopic = stream.getInputTopic();
        TestOutputTopic<String, GenericRecord> customerOutputTopic = stream.getCustomerOutputTopic();
        TestOutputTopic<String, GenericRecord> addressOutputTopic = stream.getAddressOutputTopic();
        KafkaProducer<String, String> kafkaProducer = stream.getKafkaProducer();
        TestDataHelper dataHelper = new TestDataHelper();

        KeyValue<String, String> json = dataHelper.createCustomerEncodedJson();
        inputTopic.pipeInput(json.key, json.value);

        // verify the producer send wasn't called - only called if exceptions occur
        verify(kafkaProducer, times(0)).send(any());

        // should have one record - customer
        assertFalse(customerOutputTopic.isEmpty(), "success topic is empty");
        TestRecord<String, GenericRecord> customer = customerOutputTopic.readRecord();
        assertTrue(customerOutputTopic.isEmpty(), "customer topic is not empty");
        assertTrue(addressOutputTopic.isEmpty(), "address topic is not empty");
        dataHelper.assertCustomerEqual(json.value, customer.getValue(), null, null);
    }

    @Test
    @DisplayName("Stream invalid bae64 string")
    public void testStreamInvalidBase64() {

        TestInputTopic<String, String> inputTopic = stream.getInputTopic();
        TestOutputTopic<String, GenericRecord> customerOutputTopic = stream.getCustomerOutputTopic();
        TestOutputTopic<String, GenericRecord> addressOutputTopic = stream.getAddressOutputTopic();

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
        assertHasCorrelationId(errorRecord);

        // should have received no records
        assertTrue(customerOutputTopic.isEmpty(), "customer topic is not empty");
        assertTrue(addressOutputTopic.isEmpty(), "address topic is not empty");
    }

    @Test
    @DisplayName("Stream invalid JSON string")
    public void testStreamInvalidJson() {

        TestInputTopic<String, String> inputTopic = stream.getInputTopic();
        TestOutputTopic<String, GenericRecord> customerOutputTopic = stream.getCustomerOutputTopic();
        TestOutputTopic<String, GenericRecord> addressOutputTopic = stream.getAddressOutputTopic();

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
        assertHasCorrelationId(errorRecord);

        // should have received no records
        assertTrue(customerOutputTopic.isEmpty(), "customer topic is not empty");
        assertTrue(addressOutputTopic.isEmpty(), "address topic is not empty");
    }

    @Test
    @DisplayName("Stream expected JSON string")
    public void testStreamExpectedJson() {

        TestInputTopic<String, String> inputTopic = stream.getInputTopic();
        TestOutputTopic<String, GenericRecord> customerOutputTopic = stream.getCustomerOutputTopic();
        TestOutputTopic<String, GenericRecord> addressOutputTopic = stream.getAddressOutputTopic();
        KafkaProducer<String, String> kafkaProducer = stream.getKafkaProducer();

        String key = "1";
        // {"FirstName":"FN354","LastName":"LN8987","Age":12,"Address":{"Line1":"3942 Sesame Street","Line2":null,"City":"Omaha","State":"NE","Zip":62447}}
        String value = "eyJGaXJzdE5hbWUiOiJGTjM1NCIsIkxhc3ROYW1lIjoiTE44OTg3IiwiQWdlIjoxMiwiQWRkcmVzcyI6eyJMaW5lMSI6IjM5NDIgU2VzYW1lIFN0cmVldCIsIkxpbmUyIjpudWxsLCJDaXR5IjoiT21haGEiLCJTdGF0ZSI6Ik5FIiwiWmlwIjo2MjQ0N319";
        inputTopic.pipeInput(key, value);

        // verify the producer send wasn't called - only called if exceptions occur
        verify(kafkaProducer, times(0)).send(any());

        // should have one record - customer
        assertFalse(customerOutputTopic.isEmpty(), "customer topic is empty");
        GenericRecord customer = customerOutputTopic.readRecord().value();
        assertEquals("FN354", customer.get("firstName"));
        assertEquals("LN8987", customer.get("lastName"));
        assertEquals(12, customer.get("age"));
        assertTrue(customerOutputTopic.isEmpty(), "customer topic is not empty");
        assertFalse(addressOutputTopic.isEmpty(), "address topic is empty");
        GenericRecord address = addressOutputTopic.readRecord().value();
        assertEquals("3942 Sesame Street", address.get("addressLine1"));
        assertEquals("null", address.get("addressLine2"));
        assertEquals("Omaha", address.get("city"));
        assertEquals("NE", address.get("state"));
        assertEquals("62447", address.get("postalCode"));
        assertTrue(addressOutputTopic.isEmpty(), "address topic is not empty");
    }

    private void assertHasCorrelationId(ProducerRecord<String, String> errorRecord) {
        Headers headers = errorRecord.headers();
        assertNotNull(headers);
        Iterable<Header> headerValues = headers.headers("Correlation-Id");
        assertNotNull(headerValues);
        Iterator<Header> headerValuesIter = headerValues.iterator();
        assertTrue(headerValuesIter.hasNext());
        log.info("Correlation-Id: {}", headerValuesIter.next().value().toString());
    }
}
