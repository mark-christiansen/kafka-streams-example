package com.machrist.kafka.streams;

import com.machrist.kafka.streams.util.SerdeCreator;

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

public class TopologyBuilder {

    private static final Logger log = LoggerFactory.getLogger(TopologyBuilder.class);

    private final String inputTopic;
    private final String outputTopic;
    private final String failureOutputTopic;
    private final SerdeCreator serdes;
    private final KafkaProducer<Long, GenericRecord> errorHandler;

    public TopologyBuilder(Properties applicationProperties,
                           SerdeCreator serdes,
                           KafkaProducer<Long, GenericRecord> errorHandler) {
        this.serdes = serdes;
        this.inputTopic = applicationProperties.getProperty("input.topic");
        this.outputTopic = applicationProperties.getProperty("output.topic");
        this.failureOutputTopic = applicationProperties.getProperty("failure.output.topic");
        this.errorHandler = errorHandler;
    }

    public Topology build(Properties streamProperties) {

        final StreamsBuilder builder = new StreamsBuilder();

        log.info("Subscribing to input topic {}", inputTopic);
        builder.stream(inputTopic,
                Consumed.with(Serdes.Long(), serdes.createGenericSerde(false)))
                .map((k, v) -> {
                    try {
                        v.put("firstName", v.get("firstName").toString().toUpperCase());
                        v.put("lastName", v.get("lastName").toString().toUpperCase());
                        return new KeyValue<>(k, v);
                    } catch (Exception e) {
                        String correlationId = UUID.randomUUID().toString();
                        log.error(format("Error encountered transforming message [correlationId=%s, key=%s, value=%s]", correlationId, k, v), e);
                        // send message to dead letter queue and then send a null message down the stream to ignore this
                        // message
                        List<Header> headers = Collections.singletonList(new RecordHeader("Correlation-Id", correlationId.getBytes()));
                        errorHandler.send(new ProducerRecord<Long, GenericRecord>(failureOutputTopic, 0, k, v, headers));
                        return null;
                    }
                })
                .to(outputTopic, Produced.with(Serdes.Long(), serdes.createGenericSerde(false)));

        return builder.build(streamProperties);
    }
}
