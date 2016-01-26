# biryani

<h1> Installing </h1>

Step 1: 
  Install docker.

Step 2:
  Download the local copy of the biryani repo.
  Go to the folder corenlp and run the following command.
  ```
  docker build -t image-name . 
  ```
  Note: There is a period after image-name, which specifies that Docker file is in current directory
  Example: ``` docker build -t phani\ccnlp:1.0 . ```
  
Step 3:
  To run the image created
  <h3>docker run image-name java -cp ".:lib/*" consumer #threads #documents </h3>
  Example: docker run phani\ccnlp:1.0 java -cp ".:lib/*" consumer 16 200

# queue/queue_producer_consumer.py
A python module contining two functions producer and consumer. Each function reads the configuration file and executes accordingly.  
Consumer function generates a RabbitMQ queue by fetching data form the MongoDB.  
Procuder function consumes the messages in the queue and processed accordingly.  
  
<b>import the module in your code using</b>   
import QueueProcuder_Consumer as queue  
<b>calling consumer</b>   
queue.consumer()    
<b>calling producer</b>    
queue.producer()    

# queue/queue.json

This the configuration file needed for the python module.

# corenlp/Dockerfile

A Dockerfile which builds a corenlp container

# corenlp/corenlp.json

Configuration file which specifies how the output of the corenlp container to be processed

# petrach/Dockerfile

A Dockerfile which builds a pertach container

# retrach/petrach.json

A configuration file which specifies how the output of the petrach container to be processed
