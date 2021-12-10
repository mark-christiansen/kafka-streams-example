package com.machrist.kafka.streams;

import com.machrist.kafka.streams.util.SerdeCreator;
import com.machrist.kafka.model.Address;
import com.machrist.kafka.model.Customer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static java.lang.String.format;
import static java.lang.String.valueOf;

public class TopologyBuilder {

    private static final Logger log = LoggerFactory.getLogger(TopologyBuilder.class);

    private static final String OUTPUT_TYPE = "OUTPUT_TYPE-";
    private static final String CUSTOMER = "CUSTOMER";
    private static final String ADDRESS = "ADDRESS";

    private final String inputTopic;
    private final String customerOutputTopic;
    private final String addressOutputTopic;
    private final String failureOutputTopic;
    private final SerdeCreator serdes;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final KafkaProducer<String, String> errorHandler;

    public TopologyBuilder(Properties applicationProperties,
                           SerdeCreator serdes,
                           KafkaProducer<String, String> errorHandler) {
        this.serdes = serdes;
        this.inputTopic = applicationProperties.getProperty("input.topic");
        this.customerOutputTopic = applicationProperties.getProperty("customer.output.topic");
        this.addressOutputTopic = applicationProperties.getProperty("address.output.topic");
        this.failureOutputTopic = applicationProperties.getProperty("failure.output.topic");
        this.errorHandler = errorHandler;
    }

    public Topology build(Properties streamProperties) {

        final StreamsBuilder builder = new StreamsBuilder();

        log.debug("Subscribing to input topic {}", inputTopic);
        Map<String, KStream<String, GenericRecord>> streams = builder.stream(inputTopic,
                Consumed.with(Serdes.String(), Serdes.String()))
                .flatMap((k, v) -> {
                    try {

                        List<KeyValue<String, GenericRecord>> records = new ArrayList<>();

                        // the value is a bas64 string surrounded in double quotes - first strip the quotes out
                        String valueMinusQuotes = v;
                        if (v.startsWith("\"") && v.endsWith("\"")) {
                            valueMinusQuotes = v.substring(1, v.length()-1);
                        }

                        // Base64 decode the incoming string message and read as JSON node tree
                        String json = new String(Base64.getDecoder().decode(valueMinusQuotes));
                        JsonNode node = objectMapper.readTree(json);

                        // get customer record from JSON node
                        Customer customer = new Customer();
                        customer.setId(node.get("Id") != null ? node.get("Id").asText() : UUID.randomUUID().toString());
                        customer.setFirstName(node.get("FirstName") != null ? node.get("FirstName").asText() : null);
                        customer.setLastName(node.get("LastName") != null ? node.get("LastName").asText() : null);
                        customer.setAge(node.get("Age") != null ? node.get("Age").intValue() : null);
                        records.add(new KeyValue<>(customer.getId(), customer));

                        // get address 1 record from JSON node
                        final JsonNode address1 = node.get("Address");
                        if (address1 != null) {
                            Address address = getAddress(address1);
                            records.add(new KeyValue<>(address.getId(), address));
                        }

                        // get address 2 record from JSON node
                        final JsonNode address2 = node.get("Address2");
                        if (address2 != null) {
                            Address address = getAddress(address2);
                            records.add(new KeyValue<>(address.getId(), address));
                        }

                        return records;

                    } catch (Exception e) {
                        String correlationId = UUID.randomUUID().toString();
                        log.error(format("Error encountered transforming message [correlationId=%s, key=%s, value=%s]", correlationId, k, v), e);
                        // send message to dead letter queue and then send a null message down the stream to ignore this
                        // message
                        List<Header> headers = Arrays.asList(new RecordHeader("Correlation-Id", correlationId.getBytes()));
                        errorHandler.send(new ProducerRecord<>(failureOutputTopic, 0, k, v, headers));
                        return new ArrayList<>();
                    }
                })
                .split(Named.as(OUTPUT_TYPE))
                .branch((k, v) -> v.getSchema().getFullName().equals(Customer.class.getName()), Branched.as(CUSTOMER))
                .branch((k, v) -> v.getSchema().getFullName().contains(Address.class.getName()), Branched.as(ADDRESS))
                .noDefaultBranch();

        streams.get(OUTPUT_TYPE + CUSTOMER).to(customerOutputTopic, Produced.with(Serdes.String(), serdes.createGenericSerde(false)));
        streams.get(OUTPUT_TYPE + ADDRESS).to(addressOutputTopic, Produced.with(Serdes.String(), serdes.createGenericSerde(false)));

        return builder.build(streamProperties);
    }

    private Address getAddress(JsonNode node) {
        Address address = new Address();
        address.setId(node.get("Id") != null ? node.get("Id").asText() : UUID.randomUUID().toString());
        address.setAddressLine1(node.get("Line1") != null ? node.get("Line1").asText() : null);
        address.setAddressLine2(node.get("Line2") != null ? node.get("Line2").asText() : null);
        address.setCity(node.get("City") != null ? node.get("City").asText() : null);
        address.setState(node.get("State") != null ? node.get("State").asText() : null);
        address.setPostalCode(node.get("Zip") != null ? node.get("Zip").asText() : null);
        return address;
    }
}
