import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Properties;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import com.rabbitmq.client.*;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
public class consumer {

	private static final consumer instance;
	private StanfordCoreNLP pipeline = null;
	private JSONParser parser = new JSONParser();
	private ArrayList<Annotation> annotate_list= new ArrayList<Annotation>();

	static {
		instance = new consumer();
		Properties props = new Properties();
		props.put("annotators", "tokenize, ssplit,parse");
		props.put("parse.mode",
				"edu/stanford/nlp/models/srparser/englishSR.ser.gz");

		instance.pipeline = new StanfordCoreNLP(props);
	}
	public static void main(String[] argv) throws Exception {
		// Read the configuration file of corenlp
		String R_ip = "";
		int R_port = 0;
		String R_usr = "";
		String R_pass = "";
		String R_vhost = "";
		String R_queue = "";
		try {
			FileReader reader = new FileReader(
					"C:\\Users\\Phani\\workspace\\Rabbitmq\\src\\config.json");
			JSONObject jsonobject = (JSONObject) new JSONParser().parse(reader);
			JSONObject rabbit = (JSONObject) jsonobject.get("rabbitmq");
			//JSONObject mongo = (JSONObject) jsonobject.get("mongodb");
			R_ip = (String) rabbit.get("ip");
			R_port = Integer.parseInt((String) rabbit.get("port"));
			R_usr = (String) rabbit.get("username");
			R_pass = (String) rabbit.get("password");
			R_vhost = (String) rabbit.get("vhost");
			R_queue = (String) rabbit.get("queue");
		} catch (Exception e) {
			e.printStackTrace();
		}
		// RabbitMQ code
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(R_ip);
		factory.setVirtualHost(R_vhost);
		factory.setPort(R_port);
		factory.setUsername(R_usr);
		factory.setPassword(R_pass);
		final Connection connection = factory.newConnection();
		final Channel channel = connection.createChannel();
		channel.queueDeclare(R_queue, true, false, false, null);
		channel.basicQos(1);
		Consumer consumer_rabbimq = new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope,
					AMQP.BasicProperties properties, byte[] body)
					throws IOException {
				String message = new String(body, "UTF-8");
				// System.out.println(" [x] Received  messages'");
				try { 
					doWork(message);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} finally { 
					channel.basicAck(envelope.getDeliveryTag(), false);
				}
			}
		};
		channel.basicConsume(R_queue, false, consumer_rabbimq);
	}
	private static void doWork(String input) throws ParseException {
		JSONObject json = (JSONObject) instance.parser.parse(input);
		//PrintWriter out = new PrintWriter(System.out);
		String article_body=(String) json.get("article_body");
		Annotation annotation = new Annotation(article_body);
		addAnnotation(annotation);
		if(instance.annotate_list.size()>5)
		{
			instance.pipeline.annotate(instance.annotate_list, 4);
			//System.out.println(instance.pipeline.timingInformation());
			//instance.pipeline.prettyPrint(annotation, out);
			instance.annotate_list.clear();
		}
	}
	private static void addAnnotation(Annotation annotate)
	{
		instance.annotate_list.add(annotate);
	}
}

