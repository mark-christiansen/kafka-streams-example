package com.machrist.kafka.streams;

import com.machrist.kafka.streams.util.SerdeCreator;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
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

import java.io.IOException;
import java.util.*;

import static java.lang.String.format;

public class TopologyBuilder {

    private static final Logger log = LoggerFactory.getLogger(TopologyBuilder.class);

    private final String inputTopic;
    private final String outputTopic;
    private final String failureOutputTopic;
    private final SerdeCreator serdes;
    private final KafkaProducer<Long, GenericRecord> errorHandler;
    private final SchemaRegistryClient schemaRegistryClient;

    public TopologyBuilder(Properties applicationProperties,
                           SerdeCreator serdes,
                           KafkaProducer<Long, GenericRecord> errorHandler,
                           SchemaRegistryClient schemaRegistryClient) {
        this.serdes = serdes;
        this.inputTopic = applicationProperties.getProperty("input.topic");
        this.outputTopic = applicationProperties.getProperty("output.topic");
        this.failureOutputTopic = applicationProperties.getProperty("failure.output.topic");
        this.errorHandler = errorHandler;
        this.schemaRegistryClient = schemaRegistryClient;
    }

    public Topology build(Properties streamProperties) throws RestClientException, IOException {

        String subject = outputTopic + "-value";
        final Schema outputTopicSchema = new Schema.Parser().parse(this.schemaRegistryClient.getLatestSchemaMetadata(subject).getSchema());

        log.info("Subscribing to input topic {}", inputTopic);
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(Serdes.Long(), serdes.createGenericSerde(false)))
                .map((k, v) -> {
                    try {
                        GenericRecordBuilder recordBuilder = new GenericRecordBuilder(outputTopicSchema);
                        recordBuilder.set("id", v.get("id"));
                        recordBuilder.set("fullName", v.get("firstName").toString().toUpperCase() + " " + v.get("lastName").toString().toUpperCase());
                        return new KeyValue<>(k, (GenericRecord) recordBuilder.build());
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
