package com.machrist.kafka.streams.util;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class SerdeCreator {

    private static final String SCHEMA_REGISTRY_AUTH = "schema.registry.auth";

    private final Properties kafkaProps;
    private final SchemaRegistryClient client;

    public SerdeCreator(Properties kafkaProps, SchemaRegistryClient client) {
        this.kafkaProps = kafkaProps;
        this.client = client;
    }

    public SchemaRegistryClient getSchemaRegistryClient() {
        return this.client;
    }

    public Serde<GenericRecord> createGenericSerde(boolean key) {
        GenericAvroSerde serde = new GenericAvroSerde(client);
        serde.configure(getSerdeConfig(), key);
        return serde;
    }

    private Map<String, Object> getSerdeConfig() {

        final Map<String, Object> serdeConfig = new HashMap<>();
        serdeConfig.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                kafkaProps.get(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG));
        serdeConfig.put(KafkaAvroDeserializerConfig.AUTO_REGISTER_SCHEMAS, true);
        // these settings are necessary to support heterogeneous topics
        //serdeConfig.put(KafkaAvroDeserializerConfig.KEY_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class);
        //serdeConfig.put(KafkaAvroDeserializerConfig.VALUE_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class);

        boolean auth = Boolean.parseBoolean(kafkaProps.getProperty(SCHEMA_REGISTRY_AUTH));
        if (auth) {
            serdeConfig.put(AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE,
                    kafkaProps.get(AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE));
            serdeConfig.put(AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG,
                    kafkaProps.get(AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG));
        }

        serdeConfig.put("ssl.keystore.location", kafkaProps.get("ssl.keystore.location"));
        serdeConfig.put("ssl.keystore.password", kafkaProps.get("ssl.keystore.password"));
        serdeConfig.put("ssl.truststore.location", kafkaProps.get("ssl.truststore.location"));
        serdeConfig.put("ssl.truststore.password", kafkaProps.get("ssl.truststore.password"));
        serdeConfig.put("auto.register.schemas", kafkaProps.get("auto.register.schemas"));
        serdeConfig.put("use.latest.version", kafkaProps.get("use.latest.version"));
        serdeConfig.put("latest.compatibility.strict", kafkaProps.get("latest.compatibility.strict"));

        return serdeConfig;
    }
}
