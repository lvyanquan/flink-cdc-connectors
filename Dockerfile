FROM flink
RUN mkdir -p /opt/flink-cdc
RUN mkdir -p /opt/flink/usrlib
ENV FLINK_CDC_HOME /opt/flink-cdc
COPY flink-cdc-dist/target/flink-cdc-3.0-SNAPSHOT-bin.tar.gz /tmp/
RUN tar -xzvf /tmp/flink-cdc-3.0-SNAPSHOT-bin.tar.gz -C /tmp/ && \
    mv /tmp/flink-cdc-3.0-SNAPSHOT/* /opt/flink-cdc/ && \
    rm -rf /tmp/flink-cdc-3.0-SNAPSHOT /tmp/flink-cdc-3.0-SNAPSHOT-bin.tar.gz
# copy jars to cdc libs
COPY flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-values/target/flink-cdc-pipeline-connector-values-3.0-SNAPSHOT.jar /opt/flink/usrlib/flink-cdc-pipeline-connector-values-3.0-SNAPSHOT.jar
COPY flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-mysql/target/flink-cdc-pipeline-connector-mysql-3.0-SNAPSHOT.jar /opt/flink/usrlib/flink-cdc-pipeline-connector-mysql-3.0-SNAPSHOT.jar
# copy flink cdc pipeline conf file, Here is an example. Users can replace it according to their needs.
COPY mysql-doris.yaml $FLINK_CDC_HOME/conf
