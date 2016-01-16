FROM java:8
ADD . /
RUN wget -qO- -O tmp.zip http://nlp.stanford.edu/software/stanford-corenlp-full-2015-12-09.zip && \
     unzip -o tmp.zip && \
     wget https://www.rabbitmq.com/releases/rabbitmq-java-client/v3.6.0/rabbitmq-java-client-bin-3.6.0.zip && \
     unzip -o rabbitmq-java-client-bin-3.6.0.zip && \
     wget https://json-simple.googlecode.com/files/json-simple-1.1.1.jar  && \
     mkdir -p lib && mv json-simple-1.1.1.jar  lib && mv stanford-corenlp-full-2015-12-09/*jar lib && mv rabbitmq-java-client-bin-3.6.0/*jar lib 
CMD javac -cp "lib/*" consumer.java && \
     java -cp ".:lib/*" consumer

