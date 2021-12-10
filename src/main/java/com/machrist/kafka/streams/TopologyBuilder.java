package com.machrist.kafka.streams;

import com.machrist.kafka.streams.util.SerdeCreator;
import com.machrist.kafka.model.Address;
import com.machrist.kafka.model.Customer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static java.lang.String.format;

public class TopologyBuilder {

    private static final Logger log = LoggerFactory.getLogger(TopologyBuilder.class);

    private final String inputTopic;
    private final String successOutputTopic;
    private final String failureOutputTopic;
    private final SerdeCreator serdes;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final KafkaProducer<String, String> errorHandler;

    public TopologyBuilder(Properties applicationProperties,
                           SerdeCreator serdes,
                           KafkaProducer<String, String> errorHandler) {
        this.serdes = serdes;
        this.inputTopic = applicationProperties.getProperty("input.topic");
        this.successOutputTopic = applicationProperties.getProperty("success.output.topic");
        this.failureOutputTopic = applicationProperties.getProperty("failure.output.topic");
        this.errorHandler = errorHandler;
    }

    public Topology build(Properties streamProperties) {

        final StreamsBuilder builder = new StreamsBuilder();

        log.debug("Subscribing to input topic {}", inputTopic);
        builder.stream(inputTopic,
                Consumed.with(Serdes.String(), Serdes.String()))
                .flatMap((k, v) -> {
                    try {

                        List<KeyValue<String, GenericRecord>> records = new ArrayList<>();

                        // Base64 decode the incoming string message and read as JSON node tree
                        String json = new String(Base64.getDecoder().decode(v));
                        JsonNode node = objectMapper.readTree(json);

                        // get customer record from JSON node
                        Customer customer = new Customer();
                        customer.setId(node.get("Id") != null ? node.get("Id").asText() : null);
                        customer.setFirstName(node.get("FirstName") != null ? node.get("FirstName").asText() : null);
                        customer.setLastName(node.get("LastName") != null ? node.get("LastName").asText() : null);
                        records.add(new KeyValue<>(customer.getId(), customer));

                        // get address 1 record from JSON node
                        final JsonNode address1 = node.get("Address1");
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
                        log.error(format("Error encountered transforming message [key=%s, value=%s]", k, v), e);
                        // send message to dead letter queue and then send a null message down the stream to ignore this
                        // message
                        errorHandler.send(new ProducerRecord<>(failureOutputTopic, k, v));
                        return new ArrayList<>();
                    }
                })
                .to(successOutputTopic, Produced.with(Serdes.String(), serdes.createGenericSerde(false)));

        return builder.build(streamProperties);
    }

    private Address getAddress(JsonNode node) {
        Address address = new Address();
        address.setId(node.get("Id") != null ? node.get("Id").asText() : null);
        address.setAddressLine1(node.get("AddressLine1") != null ? node.get("AddressLine1").asText() : null);
        address.setAddressLine2(node.get("AddressLine2") != null ? node.get("AddressLine2").asText() : null);
        address.setAddressLine3(node.get("AddressLine3") != null ? node.get("AddressLine3").asText() : null);
        address.setCity(node.get("City") != null ? node.get("City").asText() : null);
        address.setState(node.get("State") != null ? node.get("State").asText() : null);
        address.setCountry(node.get("Country") != null ? node.get("Country").asText() : null);
        address.setPostalCode(node.get("PostalCode") != null ? node.get("PostalCode").asText() : null);
        return address;
    }
}
