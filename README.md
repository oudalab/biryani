# biryani

<h1> Installing </h1>

<b>Step 1:</b> 
  Install docker.
  
<b>Step 2:</b> 
  Install python.
  
<b>Step 3:</b>
  Install RabbitMQ
  
 <b>Step 4: </b>
   Choosing Brach
   
   master brach is the ligth weight version of biryani which has only specified annotators(annotators, tokenize, ssplit, pos, parse)
   
   If you want to run biryani will all annotators refer to brach All-Annotators
   
   If you want to run biryani will all annotators and also dynamically decide how many documents to be processed using kalman filter refer to brach kalman_filter_all_anno
  
<b>Step 5: </b>
  Sending documents to the RabbitMQ queue
  
  Download the local copy of the biryani repo. Open the file ```producer.py``` and make the necessary changes according to how 
  your RabbitMQ server is setup
  
  Once the changes to the ```producer.py```  are complete run the file using the following command
  ```python producer.py```

<b>Step 6:</b>
Making changes to corenlp.json and log4j.properties files

Go to ```biryani/corenlp/``` folder, you can find ```corenlp.json``` and ```log4j.properties``` files<br>

```corenlp.json``` file contains the RabbitMq server configuration information and queue name in which the documents are present<br>
Make sure you make neccessary changes to the ```corenlp.json``` file according to how you setup your RabbitMQ server and Queue name<br>

```log4j.xml``` file containes the logging configuration details. Make the necessary changes for the ip address and port you want to use for logging.<br>
```
<Socket name="socket" host="logstash server host" port="5000">
```

<b> Step 7:</b>
docker-elk<br>
Download the docker-elk repo from <br>
https://github.com/deviantony/docker-elk

Go to the directory ```docker-elk/logstash/config```<br>
you will find ```logstash.conf``` 

add the following code below the tcp in ```logstash.conf``` file
```
log4j 
{
  port => the port number you added in log4j.xml file
}
```
<i>Note:</i> Make sure that you add the port number in the ```docker-compose.yml``` file of the root directory.<br>
you can find ports section in the file, just add the port you added in ```logstash.conf``` here.

<b>Step 8:</b>
  
  Go to the folder corenlp and run the following command.
  
  ```docker build -t image-name . ```
  
  <i>Note:</i> There is a period after image-name, which specifies that Docker file is in current directory. 
  
  Example: ``` docker build -t phani\ccnlp:1.0 . ```
  
<b>Step 9:</b>
  To run the image created
  
  ```docker run image-name java -cp ".:"lib/*" corenlp_worker #threads #documents(batch size) #Log_token #Database Name ```
  
  <i>Note:</i> Be careful with the image name you give. If the image is not present, docker searches for the image in the dockerhub and if there an image it would download the image and run the for you.
  
  Example: ```docker run phani\ccnlp:1.0 java -cp ".:"lib/*" corenlp_worker 16 200 logging test_database```

<b> Step 10:</b>
Install Petrarch2<br>

Install petrarch2 by using the following command.<br>
```
pip install git+https://github.com/openeventdata/petrarch2.git
```

<b> Step 11: </b>
Extracting phrases from corenlp parsed tree and storing them in mongodb<br>

Once the container has parsed all the documents copy the database file to ```biryani/utilities/``` directory<br>
In the directory you can find ```getPhrases_threads.py```. Run the following command
```
python getPhrases_threads.py corenlp_databasefile.db # documents to be processed per batch  #threads
```
Example
```
python getPhrases_threads.py test_database.db 5000 16
```

The extarcted phrases are stored in ```test_database_petrarch.db```
