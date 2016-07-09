# biryani

<h1> Installing </h1>

<b>Step 1:</b> 
  Install docker.
  
<b>Step 2:</b>
  Install RabbitMQ

<b>Step 3:</b>
  Download the local copy of the biryani repo.
  Go to the folder corenlp and run the following command.
  
  ```docker build -t image-name . ```
  
  <i>Note:</i> There is a period after image-name, which specifies that Docker file is in current directory. 
  
  Example: ``` docker build -t phani\ccnlp:1.0 . ```
  
<b>Step 3:</b>
  To run the image created
  
  ```docker run image-name java -cp ".:"lib/*:lib/hyperic-sigar-1.6.4/sigar-bin/lib/*" consumer #threads #documents(batch size) #Log_token #Database Name ```
  
  <i>Note:</i> Be careful with the image name you give. If the image is not present, docker searches for the image in the dockerhub and if there an image it would download the image and run the for you.
  
  Example: ```docker run phani\ccnlp:1.0 java -cp ".:lib/*" consumer 16 200 logging test_database```
  
  
#Logging

Logging for the repo is setup by using [docker-elk](https://github.com/deviantony/docker-elk).

Walk through the instruction and make sure you open the port for logstash in you docker-compose.yml file.


