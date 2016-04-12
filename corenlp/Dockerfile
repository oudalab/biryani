FROM java:8
ADD . /
RUN wget -qO- -O tmp.zip http://nlp.stanford.edu/software/stanford-corenlp-full-2015-12-09.zip && \
     unzip -o tmp.zip && \
     wget http://nlp.stanford.edu/software/stanford-srparser-2014-10-23-models.jar && \
     wget https://www.rabbitmq.com/releases/rabbitmq-java-client/v3.6.0/rabbitmq-java-client-bin-3.6.0.zip && \
     unzip -o rabbitmq-java-client-bin-3.6.0.zip && \
     wget https://json-simple.googlecode.com/files/json-simple-1.1.1.jar  && \
     wget https://repo1.maven.org/maven2/com/google/guava/guava/19.0/guava-19.0.jar && \
     wget http://apache.mirrors.pair.com/logging/log4j/1.2.17/log4j-1.2.17.zip && \
     unzip -o log4j-1.2.17.zip && \
     wget https://oss.sonatype.org/content/repositories/releases/org/mongodb/mongo-java-driver/3.0.4/mongo-java-driver-3.0.4.jar && \
     wget https://oss.sonatype.org/content/repositories/releases/org/mongodb/bson/3.0.4/bson-3.0.4.jar && \
     wget https://bitbucket.org/xerial/sqlite-jdbc/downloads/sqlite-jdbc-3.8.11.2.jar && \
     wget http://heanet.dl.sourceforge.net/project/sigar/sigar/1.6/hyperic-sigar-1.6.4.zip && \
     unzip -o hyperic-sigar-1.6.4.zip && \
     mkdir -p lib && mv json-simple-1.1.1.jar lib && \
     mv guava-19.0.jar  lib && \
     mv stanford-corenlp-full-2015-12-09/*jar lib && \
     mv stanford-srparser-2014-10-23-models.jar lib && \
     mv rabbitmq-java-client-bin-3.6.0/*jar lib && \
     mv apache-log4j-1.2.17/*jar lib && \
     mv mongo-java-driver-3.0.4.jar lib && \
     mv bson-3.0.4.jar lib && \
     mv hyperic-sigar-1.6.4 lib && \
     mv sqlite-jdbc-3.8.11.2.jar lib
RUN  javac -cp "lib/*:lib/hyperic-sigar-1.6.4/sigar-bin/lib/*" sqlite_reader.java consumer.java consumer_norestart.java
