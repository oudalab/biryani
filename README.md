# biryani


# Queue/QueueProducer_Consumer.py
A python module contining two functions producer and consumer. Each function reads the configuration file and executes accordingly.  
Consumer function generates a RabbitMQ queue by fetching data form the MongoDB.  
Procuder function consumes the messages in the queue and processed accordingly.  
  
import the module in your code using   
import QueueProcuder_Consumer as queue  
<b>calling consumer</b>   
queue.consumer()    
calling producer    
queue.producer()     
