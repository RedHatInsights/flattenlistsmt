#!/bin/sh

docker run -it --net=host --rm confluentinc/cp-schema-registry \
                               /usr/bin/kafka-avro-console-consumer \
                               --bootstrap-server=localhost:9092 \
                               --from-beginning \
                               --topic dbserver1.public.hosts
