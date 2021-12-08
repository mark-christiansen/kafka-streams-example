FROM openjdk:11-jdk-slim

ARG JAR_FILE

RUN /bin/bash -c "echo $JAR_FILE; echo $JMX_EXPORTER_JAR; echo $JMX_EXPORTER_CONFIG"
COPY target/${JAR_FILE} /app/app.jar
# unpacked archive is slightly faster on startup than running from an unexploded archive
RUN /bin/bash -c 'cd /app; jar xf app.jar'

ENTRYPOINT exec java $JAVA_OPTS -cp /app/BOOT-INF/classes:/app/BOOT-INF/lib/* com.machrist.kafka.streams.Application