FROM flink
RUN mkdir -p /opt/flink-cdc
ENV FLINK_CDC_HOME /opt/flink-cdc
COPY flink-cdc-dist/target/flink-cdc-3.0-SNAPSHOT-bin.tar.gz /tmp/
RUN tar -xzvf /tmp/flink-cdc-3.0-SNAPSHOT-bin.tar.gz -C /tmp/ && \
    mv /tmp/flink-cdc-3.0-SNAPSHOT/* $FLINK_CDC_HOME/ && \
    rm -rf /tmp/flink-cdc-3.0-SNAPSHOT /tmp/flink-cdc-3.0-SNAPSHOT-bin.tar.gz
# copy jars to cdc libs
#RUN wget -P $FLINK_CDC_HOME/lib https://repo1.maven.org/maven2/com/ververica/flink-cdc-pipeline-connector-mysql/3.0.0/flink-cdc-pipeline-connector-mysql-3.0.0.jar
#RUN wget -P $FLINK_CDC_HOME/lib https://repo1.maven.org/maven2/com/ververica/flink-cdc-pipeline-connector-doris/3.0.0/flink-cdc-pipeline-connector-doris-3.0.0.jar
COPY flink-cdc-pipeline-connector-doris-3.0-SNAPSHOT.jar $FLINK_HOME/lib
COPY flink-cdc-pipeline-connector-mysql-3.0-SNAPSHOT.jar $FLINK_HOME/lib
COPY flink-cdc-pipeline-connector-doris-3.0-SNAPSHOT.jar $$FLINK_CDC_HOME/lib
COPY flink-cdc-pipeline-connector-mysql-3.0-SNAPSHOT.jar $$FLINK_CDC_HOME/lib
# copy flink cdc pipeline conf file, Here is an example. Users can replace it according to their needs.
COPY mysql-doirs.yaml $FLINK_CDC_HOME/conf
