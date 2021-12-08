package com.machrist.kafka.streams.streams;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Properties;

import static java.lang.String.format;

public class StreamsLifecycle {

    private static final Logger log = LoggerFactory.getLogger(StreamsLifecycle.class);
    private final String applicationId;
    private final Boolean stateStoreCleanup;
    private final Topology topology;
    private final KafkaStreams streams;

    public StreamsLifecycle(String applicationId,
                            Topology topology,
                            boolean stateStoreCleanup,
                           Properties streamsProperties) {
        this.topology = topology;
        this.applicationId = applicationId;
        this.stateStoreCleanup = stateStoreCleanup;
        streamsProperties.put(org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsProperties.put(org.apache.kafka.streams.StreamsConfig.CLIENT_ID_CONFIG, applicationId);
        this.streams = new KafkaStreams(topology, streamsProperties);
    }

    @PostConstruct
    private void construct() {

        final TopologyDescription description = topology.describe();
        log.info("=======================================================================================");
        log.info("Topology: {}", description);
        log.info("=======================================================================================");

        log.info("Starting Stream {}", applicationId);
        if (streams != null) {

            streams.setUncaughtExceptionHandler(throwable -> {
                log.error(format("Stopping the application %s due to unhandled exception", applicationId), throwable);
                return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
            });

            streams.setStateListener((newState, oldState) -> {
                if (newState == KafkaStreams.State.ERROR) {
                    throw new RuntimeException("Kafka Streams went into an ERROR state");
                }
            });

            if (stateStoreCleanup) {
                streams.cleanUp();
            }

            streams.start();
        }
    }

    @PreDestroy
    private void destroy() {
        log.warn("Closing Kafka Streams application {}", applicationId);
        if (streams != null) {
            streams.close();
        }
    }

    public Boolean isHealthy() {
        return streams.state().isRunningOrRebalancing();
    }

    public String topologyDescription() {
        return topology.describe().toString();
    }
}
