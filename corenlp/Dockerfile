FROM java:8
ADD . /
RUN wget http://nlp.stanford.edu/software/stanford-corenlp-full-2016-10-30.zip && \
     unzip -o stanford-corenlp-full-2016-10-30.zip && \
     rm stanford-corenlp-full-2016-10-30.zip && \
     wget http://nlp.stanford.edu/software/stanford-srparser-2014-10-23-models.jar && \
     wget https://www.rabbitmq.com/releases/rabbitmq-java-client/v3.6.0/rabbitmq-java-client-bin-3.6.0.zip && \
     unzip -o rabbitmq-java-client-bin-3.6.0.zip && \
     rm rabbitmq-java-client-bin-3.6.0.zip && \
     wget http://central.maven.org/maven2/com/googlecode/json-simple/json-simple/1.1.1/json-simple-1.1.1.jar  && \
     wget https://repo1.maven.org/maven2/com/google/guava/guava/19.0/guava-19.0.jar && \
     wget http://www-us.apache.org/dist/logging/log4j/2.8.1/apache-log4j-2.8.1-bin.zip && \
     unzip -o apache-log4j-2.8.1-bin.zip && \
     rm apache-log4j-2.8.1-bin.zip && \
     wget https://oss.sonatype.org/content/repositories/releases/org/mongodb/mongo-java-driver/3.0.4/mongo-java-driver-3.0.4.jar && \
     wget https://oss.sonatype.org/content/repositories/releases/org/mongodb/bson/3.0.4/bson-3.0.4.jar && \
     wget https://bitbucket.org/xerial/sqlite-jdbc/downloads/sqlite-jdbc-3.8.11.2.jar && \
     mkdir -p lib && mv json-simple-1.1.1.jar lib && \
     mv guava-19.0.jar  lib && \
     mv stanford-corenlp-full-2016-10-30/*jar lib && \
     mv stanford-srparser-2014-10-23-models.jar lib && \
     mv rabbitmq-java-client-bin-3.6.0/*jar lib && \
     mv apache-log4j-2.8.1-bin/log4j-core-2.8.1.jar apache-log4j-2.8.1-bin/log4j-api-2.8.1.jar lib && \
     mv mongo-java-driver-3.0.4.jar lib && \
     mv bson-3.0.4.jar lib && \
     mv sqlite-jdbc-3.8.11.2.jar lib && \
     rm -r stanford-corenlp-full-2016-10-30/ && \
     rm -r rabbitmq-java-client-bin-3.6.0/ && \
     rm -r apache-log4j-2.8.1-bin/
RUN  javac -cp "lib/*" batch_info.java stats.java sqlite_reader.java document.java corenlp_worker.java

