# biryani

<h1> Installing </h1>

<b>Step 1:</b> 
  Install docker.

<b>Step 2:</b>
  Download the local copy of the biryani repo.
  Go to the folder corenlp and run the following command.
  <br>
  ```docker build -t image-name . ```
  <i>Note:</i> There is a period after image-name, which specifies that Docker file is in current directory. 
  
  Example: ``` docker build -t phani\ccnlp:1.0 . ```
  
<b>Step 3:</b>
  To run the image created
  ```docker run image-name java -cp ".:lib/*" consumer #threads #documents ```
  
  <i>Note:</i> Be careful with the image name you give. If the image is not present, docker searches for the image in the dockerhub and if there an image it would download the image and run the for you.
  
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
