FROM java:8
ADD . /
RUN  wget -qO- -O tmp.zip http://nlp.stanford.edu/software/stanford-corenlp-full-2015-12-09.zip && \
     unzip -o tmp.zip && \
     wget http://nlp.stanford.edu/software/stanford-srparser-2014-10-23-models.jar && \
     wget https://www.rabbitmq.com/releases/rabbitmq-java-client/v3.6.0/rabbitmq-java-client-bin-3.6.0.zip && \
     unzip -o rabbitmq-java-client-bin-3.6.0.zip && \
     wget https://json-simple.googlecode.com/files/json-simple-1.1.1.jar  && \
     wget https://repo1.maven.org/maven2/com/google/guava/guava/19.0/guava-19.0.jar && \
     wget http://apache.arvixe.com/logging/log4j/1.2.17/log4j-1.2.17.zip && \
     unzip -o log4j-1.2.17.zip && \ 
     mkdir -p lib && mv json-simple-1.1.1.jar lib && \
     mv guava-19.0.jar  lib && \
     mv stanford-corenlp-full-2015-12-09/*jar lib && \
     mv stanford-srparser-2014-10-23-models.jar lib && \
     mv rabbitmq-java-client-bin-3.6.0/*jar lib && \
     mv apache-log4j-1.2.17/*jar lib
RUN     javac -cp "lib/*" consumer.java 
CMD  javac -cp "lib/*" consumer.java && java -cp ".:lib/*" consumer 16 500 yanDefault

