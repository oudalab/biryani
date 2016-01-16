import java.io.FileReader;
import java.io.IOException;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.rabbitmq.client.*;

public class consumer {

           static int count=0;
          public static void main(String[] argv) throws Exception {
             
        	
        	  //Read the configuration file of corenlp
        	  String R_ip="";
    		  int R_port=0;
    		  String R_usr="";
    		  String R_pass="";
    		  String R_vhost="";
    		  String R_queue="";
        	  try{
        		  FileReader reader= new FileReader("corenlp.json");
        		  JSONObject jsonobject = (JSONObject) new JSONParser().parse(reader);
        		  JSONObject rabbit= (JSONObject) jsonobject.get("rabbitmq");
        		  JSONObject mongo= (JSONObject) jsonobject.get("mongodb");
        		  
        		  R_ip= (String) rabbit.get("ip");
        		  R_port= Integer.parseInt((String) rabbit.get("port"));
        		  R_usr= (String) rabbit.get("username");
        		  R_pass= (String)rabbit.get ("password");
        		  R_vhost= (String) rabbit.get("vhost");
        		  R_queue=(String) rabbit.get("queue");       		  
        	  }
        	  catch(Exception e){
        		  e.printStackTrace();
        	  }
        	  
        	ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(R_ip);
            factory.setVirtualHost(R_vhost);
            factory.setPort(R_port);
            factory.setUsername(R_usr);
            factory.setPassword(R_pass);
            
            final Connection connection = factory.newConnection();
            final Channel channel = connection.createChannel();

            channel.queueDeclare(R_queue, true, false, false, null);
            System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

            channel.basicQos(1);

            final Consumer consumer = new DefaultConsumer(channel) {
              @Override
              public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");

                //System.out.println(" [x] Received  messages'");
                try {
                  doWork(message);
                } finally {
                  //System.out.println(" [x] Done");
                  channel.basicAck(envelope.getDeliveryTag(), false);
                }
              }
            };
            channel.basicConsume(R_queue, false, consumer);
          }

          private static void doWork(String task) {
			
                 System.out.println("messages received\t"+ ++count);
          }

}

