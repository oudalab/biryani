import java.io.IOException;
import com.rabbitmq.client.*;

public class consumer {

        private static final String TASK_QUEUE_NAME = "main_queue";
           static int count=0;
          public static void main(String[] argv) throws Exception {
             
        	  
        	  
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("rabbitmq ip address");
            factory.setVirtualHost("/");
            factory.setPort(port nmber);
            factory.setUsername("username");
            factory.setPassword("password");
            final Connection connection = factory.newConnection();
            final Channel channel = connection.createChannel();

            channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);
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
            channel.basicConsume(TASK_QUEUE_NAME, false, consumer);
          }

          private static void doWork(String task) {

                 System.out.println("messages received\t"+count++);
          }

}

