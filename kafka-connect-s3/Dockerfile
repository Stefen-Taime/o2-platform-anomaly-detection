FROM quay.io/strimzi/kafka:0.51.0-kafka-4.2.0

USER root

# Download Confluent S3 Sink connector and dependencies
# Exclude Kafka libs (already provided by base image) to avoid version conflicts
RUN mkdir -p /opt/kafka/plugins/s3-sink && \
    cd /tmp && \
    curl -fL -o kafka-connect-s3-10.5.25.jar \
      https://packages.confluent.io/maven/io/confluent/kafka-connect-s3/10.5.25/kafka-connect-s3-10.5.25.jar && \
    curl -fL -o kafka-connect-s3-10.5.25.pom \
      https://packages.confluent.io/maven/io/confluent/kafka-connect-s3/10.5.25/kafka-connect-s3-10.5.25.pom && \
    # Install maven to resolve deps
    curl -fL -o maven.tar.gz https://archive.apache.org/dist/maven/maven-3/3.9.6/binaries/apache-maven-3.9.6-bin.tar.gz && \
    tar xzf maven.tar.gz && \
    # Create settings with Confluent repo
    echo '<settings><profiles><profile><id>d</id><repositories><repository><id>confluent</id><url>https://packages.confluent.io/maven/</url></repository></repositories></profile></profiles><activeProfiles><activeProfile>d</activeProfile></activeProfiles></settings>' > /tmp/settings.xml && \
    # Download dependencies EXCLUDING Kafka libs (provided by runtime)
    /tmp/apache-maven-3.9.6/bin/mvn dependency:copy-dependencies \
      -s /tmp/settings.xml \
      -DoutputDirectory=/opt/kafka/plugins/s3-sink \
      -DexcludeGroupIds=org.apache.kafka,org.scala-lang,org.slf4j,ch.qos.logback \
      -f /tmp/kafka-connect-s3-10.5.25.pom && \
    # Copy the main connector jar
    cp /tmp/kafka-connect-s3-10.5.25.jar /opt/kafka/plugins/s3-sink/ && \
    # Cleanup
    rm -rf /tmp/apache-maven-3.9.6 /tmp/maven.tar.gz /tmp/settings.xml /tmp/kafka-connect-s3-10.5.25.pom && \
    rm -rf /root/.m2

USER 1001
