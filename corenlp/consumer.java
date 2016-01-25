import java.io.FileReader;
import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import com.rabbitmq.client.*;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.PartOfSpeechAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.trees.TreeCoreAnnotations.TreeAnnotation;
import edu.stanford.nlp.util.CoreMap;
import java.util.function.Consumer;

public class consumer {

	private static final consumer instance;
	private StanfordCoreNLP pipeline = null;
	private JSONParser parser = new JSONParser();
	private ArrayList<Annotation> annotate_list= new ArrayList<Annotation>();
	
	static {
		instance = new consumer();
		Properties props = new Properties();
		props.put("annotators", "tokenize, ssplit,pos,parse");
		props.put("parse.model","edu/stanford/nlp/models/srparser/englishSR.ser.gz");

		instance.pipeline = new StanfordCoreNLP(props);
	}
	public static void main(String[] argv) throws Exception {
		// Read the configuration file of corenlp
		final int num_proc ;
		final int num_docs;
		if (argv.length==1)
		{
			num_proc=Integer.parseInt(argv[0]);
			num_docs=1;
		}
		else if(argv.length==2)
		{
			num_proc=Integer.parseInt(argv[0]);
			num_docs=Integer.parseInt(argv[1]);
		}
		else
		{
			num_proc=1;
			num_docs=1;
		}
		String R_ip = "";
		int R_port = 0;
		String R_usr = "";
		String R_pass = "";
		String R_vhost = "";
		String R_queue = "";
		try {
			FileReader reader = new FileReader("corenlp.json");
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
		try
		{
			final Connection connection = factory.newConnection();
			final Channel channel = connection.createChannel();
			channel.queueDeclare(R_queue, true, false, false, null);
			channel.basicQos(1);
			DefaultConsumer consumer_rabbimq = new DefaultConsumer(channel) 
			{
				@Override
				public void handleDelivery(String consumerTag, Envelope envelope,
						AMQP.BasicProperties properties, byte[] body) throws IOException 
				{
					String message = new String(body, "UTF-8");
					// System.out.println(" [x] Received  messages'");
					try 
					{ 
						doWork(message,num_proc,num_docs);
					} catch (ParseException e) 
					{
					// TODO Auto-generated catch block
					e.printStackTrace();
					} 	
					finally 
					{ 
						channel.basicAck(envelope.getDeliveryTag(), false);
					}
				}
			};
			channel.basicConsume(R_queue, false, consumer_rabbimq);
		}catch(ConnectException e)
		{
			e.printStackTrace();
		}
		finally
		{
			
		}
	}
	private static void doWork(String input,int num_proc,int num_docs) throws ParseException {
		JSONObject json = (JSONObject) instance.parser.parse(input);
		//PrintWriter out = new PrintWriter(System.out);
		String doc_id= (String)json.get("doc_id");
		//System.out.println(doc_id);
		String article_body=(String) json.get("article_body");
		//System.out.println(article_body);
		Annotation annotation = new Annotation(article_body);
		annotation.set(CoreAnnotations.DocIDAnnotation.class, doc_id);
		instance.annotate_list.add(annotation);
		if(instance.annotate_list.size()>=num_docs)
		{
			final int size=instance.annotate_list.size();
			//System.out.println("Number of threads: "+num_proc);
			instance.pipeline.annotate(instance.annotate_list,num_proc , new Consumer<Annotation>() {

				public void accept(Annotation arg0) 
				{
					
					//System.out.println("---------PROCESSING PARSED TREE FOR DOCUMENT---------");
					String doc_id= arg0.get(CoreAnnotations.DocIDAnnotation.class);
					//System.out.println("DOC_ID:: "+doc_id);
					List<CoreMap> sentences = arg0.get(SentencesAnnotation.class);
					for(CoreMap sentence: sentences) 
					{
						
						for (CoreLabel token: sentence.get(TokensAnnotation.class)) 
						{
							String word = token.get(TextAnnotation.class);
							String pos = token.get(PartOfSpeechAnnotation.class);
							String ne = token.get(NamedEntityTagAnnotation.class);
						}
						Tree tree = sentence.get(TreeAnnotation.class);
						//System.out.println(sentence+"\n"+tree);
						//SemanticGraph dependencies = sentence.get(CollapsedCCProcessedDependenciesAnnotation.class);
						//System.out.println(dependencies);
					}
					System.out.println("processd:"+sentences.size());
				}
			});
			//System.out.println(instance.pipeline.timingInformation());
			//instance.pipeline.prettyPrint(annotation, out);
			instance.annotate_list.clear();	 
			StanfordCoreNLP.clearAnnotatorPool();
		}
	}
}

