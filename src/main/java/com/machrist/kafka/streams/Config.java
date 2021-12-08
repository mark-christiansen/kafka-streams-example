package com.machrist.kafka.streams;

import com.machrist.kafka.streams.streams.StreamsLifecycle;
import com.machrist.kafka.streams.util.SerdeCreator;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.kafka.streams.Topology;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Configuration
public class Config {

    private static final String SCHEMA_REGISTRY_URL = "schema.registry.url";
    private static final String SCHEMA_CACHE_CAPACITY = "schema.cache.capacity";

    @Bean
    @ConfigurationProperties(prefix = "kafka.streams")
    public Properties kafkaProperties() {
        return new Properties();
    }

    @Bean(name = "applicationProperties")
    @ConfigurationProperties(prefix = "application")
    public Properties applicationProperties() {
        return new Properties();
    }

    @Bean
    public SchemaRegistryClient schemaRegistryClient() {

        Properties kafkaProperties = kafkaProperties();
        // pull out schema registry properties from kafka properties to pass to schema registry client
        Map<String, Object> schemaProperties = new HashMap<>();
        for (Map.Entry<Object, Object> entry : kafkaProperties.entrySet()) {
            String propertyName = (String) entry.getKey();
            if (propertyName.startsWith("schema.registry.")) {
                schemaProperties.put(propertyName, entry.getValue());
            }
        }
        return new CachedSchemaRegistryClient(kafkaProperties.getProperty(SCHEMA_REGISTRY_URL),
                Integer.parseInt(kafkaProperties.getProperty(SCHEMA_CACHE_CAPACITY)), schemaProperties);
    }

    @Bean
    public SerdeCreator serdeCreator() {
        return new SerdeCreator(kafkaProperties(), schemaRegistryClient());
    }

    @Bean
    public TopologyBuilder topologyBuilder() {
        return new TopologyBuilder(applicationProperties(), serdeCreator());
    }

    @Bean
    public StreamsLifecycle sStreamsLifecycle(ApplicationContext applicationContext) throws IOException {

        TopologyBuilder topologyBuilder = topologyBuilder();

        Properties applicationProperties = applicationProperties();
        final boolean stateStoreCleanup = Boolean.parseBoolean(applicationProperties.getProperty("state.store.cleanup"));
        Topology topology = topologyBuilder.build(applicationProperties);

        Properties kafkaProperties = kafkaProperties();
        final String applicationId = kafkaProperties.getProperty("application.id");
        return new StreamsLifecycle(applicationId, topology, stateStoreCleanup, kafkaProperties);
    }
}
