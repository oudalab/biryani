# biryani


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

#cCorenlp/corenlp.json

Configuration file which specifies how the output of the corenlp container to be processed

# petrach/Dockerfile

A Dockerfile which builds a pertach container

# retrach/petrach.json

A configuration file which specifies how the output of the petrach container to be processed
