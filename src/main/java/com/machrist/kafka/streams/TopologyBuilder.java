package com.machrist.kafka.streams;

import com.machrist.kafka.streams.util.SerdeCreator;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

public class TopologyBuilder {

    private static final Logger log = LoggerFactory.getLogger(TopologyBuilder.class);

    private static final String BRANCH_PREFIX = "branch-";
    private static final String VALID_BRANCH = "valid";
    private static final String INVALID_BRANCH = "invalid";

    private final String inputTopic;
    private final String successOutputTopic;
    private final String failureOutputTopic;
    private final SerdeCreator serdes;

    public TopologyBuilder(Properties applicationProperties,
                           SerdeCreator serdes) {
        this.serdes = serdes;
        this.inputTopic = applicationProperties.getProperty("input.topic");
        this.successOutputTopic = applicationProperties.getProperty("success.output.topic");
        this.failureOutputTopic = applicationProperties.getProperty("failure.output.topic");
    }

    public Topology build(Properties streamProperties) {

        final StreamsBuilder builder = new StreamsBuilder();

        log.debug("Subscribing to input topic {}", inputTopic);
        Map<String, KStream<GenericRecord, GenericRecord>> input = builder.stream(inputTopic,
                Consumed.with(serdes.createGenericSerde(true), serdes.createGenericSerde(false)))
                .split(Named.as(BRANCH_PREFIX))
                .branch((k, v) -> {
                    String customerName = (String) v.get("CUST_NAME");
                    return customerName == null;
                }, Branched.as(INVALID_BRANCH))
                .defaultBranch(Branched.as(VALID_BRANCH));

        input.get(BRANCH_PREFIX + INVALID_BRANCH).to(failureOutputTopic,
                Produced.with(serdes.createGenericSerde(true), serdes.createGenericSerde(false)));
        input.get(BRANCH_PREFIX + VALID_BRANCH).to(successOutputTopic,
                Produced.with(serdes.createGenericSerde(true), serdes.createGenericSerde(false)));

        return builder.build(streamProperties);
    }
}
