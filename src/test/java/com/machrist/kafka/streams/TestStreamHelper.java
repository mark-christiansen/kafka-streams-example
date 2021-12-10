package com.machrist.kafka.streams;

import com.machrist.kafka.streams.util.SerdeCreator;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class TestStreamHelper {

    private static final String INPUT_TOPIC = "input-topic";
    private static final String CUSTOMER_OUTPUT_TOPIC = "customer-topic";
    private static final String ADDRESS_OUTPUT_TOPIC = "address-topic";
    private static final String FAILURE_OUTPUT_TOPIC = "failure-topic";
    private static final String BROKER_URL = "mock:9092";

    public Properties getApplicationProperties() {
        Properties appProps = new Properties();
        appProps.setProperty("input.topic", INPUT_TOPIC);
        appProps.setProperty("customer.output.topic", CUSTOMER_OUTPUT_TOPIC);
        appProps.setProperty("address.output.topic", ADDRESS_OUTPUT_TOPIC);
        appProps.setProperty("failure.output.topic", FAILURE_OUTPUT_TOPIC);
        appProps.setProperty("state.store.cleanup", "true");
        appProps.setProperty("in.memory.state.stores", "true");
        return appProps;
    }

    public TestStream createTestStream(Properties appProps, String applicationId, String schemaRegistryUrl) {

        MockSchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();

        Properties streamsProps = getStreamsProperties(applicationId);
        SerdeCreator serdeCreator = new SerdeCreator(streamsProps, schemaRegistry);

        KafkaProducer<String, String> kafkaProducer = Mockito.mock(KafkaProducer.class);
        TopologyBuilder topologyBuilder = new TopologyBuilder(appProps, serdeCreator, kafkaProducer);
        TopologyTestDriver testDriver = new TopologyTestDriver(topologyBuilder.build(streamsProps), streamsProps);

        KeyValue<Serde<String>, Serde<String>> inputSerdes = getInputSerdes();
        TestInputTopic<String, String> inputTopic = testDriver.createInputTopic(INPUT_TOPIC,
                inputSerdes.key.serializer(), inputSerdes.value.serializer());

        KeyValue<Serde<String>, GenericAvroSerde> customerOutputSerdes = getOutputSerdes(schemaRegistryUrl, schemaRegistry);
        TestOutputTopic<String, GenericRecord> customerOutputTopic = testDriver.createOutputTopic(
                CUSTOMER_OUTPUT_TOPIC, customerOutputSerdes.key.deserializer(), customerOutputSerdes.value.deserializer());

        KeyValue<Serde<String>, GenericAvroSerde> addressOutputSerdes = getOutputSerdes(schemaRegistryUrl, schemaRegistry);
        TestOutputTopic<String, GenericRecord> addressOutputTopic = testDriver.createOutputTopic(
                ADDRESS_OUTPUT_TOPIC, addressOutputSerdes.key.deserializer(), addressOutputSerdes.value.deserializer());

        return new TestStream(testDriver, inputTopic, customerOutputTopic, addressOutputTopic, kafkaProducer);
    }

    private KeyValue<Serde<String>, Serde<String>> getInputSerdes() {
        final Map<String, Object> serdeConfig = new HashMap<>();
        Serde<String> keySerde = Serdes.String();
        keySerde.configure(serdeConfig, true);
        Serde<String> valueSerde = Serdes.String();
        valueSerde.configure(serdeConfig, false);
        return new KeyValue<>(keySerde, valueSerde);
    }

    private KeyValue<Serde<String>, GenericAvroSerde> getOutputSerdes(String schemaRegistryUrl,
                                                                      MockSchemaRegistryClient schemaRegistry) {
        final Map<String, Object> serdeConfig = getSerdeConfig(schemaRegistryUrl);
        Serde<String> keySerde = Serdes.String();
        keySerde.configure(serdeConfig, true);
        GenericAvroSerde valueSerde =  new GenericAvroSerde(schemaRegistry);
        valueSerde.configure(serdeConfig, false);
        return new KeyValue<>(keySerde, valueSerde);
    }

    private Properties getStreamsProperties(String applicationId) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_URL);
        return props;
    }

    private Map<String, Object> getSerdeConfig(String schemaRegistryUrl) {
        final Map<String, Object> serdeConfig = new HashMap<>();
        serdeConfig.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        serdeConfig.put(KafkaAvroDeserializerConfig.AUTO_REGISTER_SCHEMAS, true);
        //serdeConfig.put(KafkaAvroDeserializerConfig.KEY_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class);
        //serdeConfig.put(KafkaAvroDeserializerConfig.VALUE_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class);
        return serdeConfig;
    }
}
