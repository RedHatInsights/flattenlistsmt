# Kafka connect SMT - Flatten list
This java lib implements Kafka connect SMT (Single Message Transformation) to
make flatten list from original tree structure.

## Config
Use it in connector config file like this:
~~~json
...
"transforms": "flattenList",
"transforms.flattenList.type": "com.redhat.insights.flattenlistsmt.FlattenList$Value",
"transforms.flattenList.sourceField": "tags",
"transforms.flattenList.outputField": "tags_flat"
...
~~~

## Install to Kafka Connect
After build copy file `target/kafka-connect-smt-flattenlist-0.0.1-assemble-all.jar`
to Kafka Connect container.

It can be done adding this line to Dockerfile:
~~~Dockerfile
COPY ./kafka-connect-smt-flattenlist-0.0.1-assemble-all.jar $KAFKA_CONNECT_PLUGINS_DIR
~~~

Or download current release:
~~~Dockerfile
RUN curl -fSL -o /tmp/plugin.tar.gz \
    https://github.com/RedHatInsights/flattenlistsmt/releases/download/0.0.1/kafka-connect-smt-flattenlistsmt-0.0.1.tar.gz && \
    tar -xzf /tmp/plugin.tar.gz -C $KAFKA_CONNECT_PLUGINS_DIR && \
    rm -f /tmp/plugin.tar.gz;
~~~

## Example
~~~bash
# build jar file and store to target directory
mvn package

# start example containers (kafka, postgres, elasticsearch, ...)
docker-compose up --build

# when containers started run in separate terminal:
cd dev
./connect.sh # init postgres and elasticsearch connectors
./show_topics.sh # check created topic 'dbserver1.public.hosts' in kafka
./show_es.sh # check transformed documents imported from postgres to elasticsearch

# ... stop containers
docker-compose down
~~~
