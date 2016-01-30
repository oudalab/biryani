import java.io.FileReader;
import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.google.common.base.Stopwatch;
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

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.UUID;



public class consumer {

	private static final consumer instance;
	private StanfordCoreNLP pipeline = null;
	private JSONParser parser = new JSONParser();
	private ArrayList<Annotation> annotate_list= new ArrayList<Annotation>();
	private Stopwatch doc_timer;
	private Stopwatch batch_timer;
	private Stopwatch total_timer;
	private Stopwatch parse_timer;
	

	//private Logger log = Logger.getLogger(consumer.class.getName());
	private Logger log = Logger.getLogger("log4j.properties");
	
	static {
		instance = new consumer();
		Properties props = new Properties();
		props.put("annotators", "tokenize, ssplit,pos,parse");
		props.put("parse.model","edu/stanford/nlp/models/srparser/englishSR.ser.gz");
		instance.pipeline = new StanfordCoreNLP(props);
		instance.batch_timer= Stopwatch.createStarted();
		instance.doc_timer= Stopwatch.createUnstarted();
		instance.total_timer=Stopwatch.createStarted();
		instance.parse_timer=Stopwatch.createStarted();
		
	}
	public static void main(String[] argv) throws Exception {
		// Read the configuration file of corenlp
		final int num_proc ;
		final int num_docs;
		final String log_token=argv[2];
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
						doWork(message,num_proc,num_docs,log_token);
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
			instance.log.error("Cannot connect to server, network error!");
		}
		finally
		{
			
		}
	}
	private static void doWork(String input,int num_proc,int num_docs,String log_token) throws ParseException {
		//inside of each dowork check the momory availability
		int mb=1024*1024;
	 Runtime instance=Runtime.getRuntime();

    instance.log.debug("Total Memory:"+instance.totalMemory() / mb);
    instance.log.debug("Free Memory:"+instance.freeMemory() / mb);
    instance.log.debug("Used Memory:"+(instance.totalMemory()-instance.freeMemory())/mb);
    instance.log.debug("Max Memory: "+instance.maxMemory()/mb);
    
    String freeMemory=instance.freeMemory()/mb;
    if(freeMemory<10)
    {
    	try {
            String content=LocalDateTime.now().toString();
            String uuid = UUID.randomUUID().toString();
		
            //in case it can not write the same file at the same time
			File file = new File("../f"+log_token+"_"+uuid".log");

			// if file doesnt exists, then create it
			if (!file.exists()) {
				file.createNewFile();
			}
            //he true will make sure it is being append
			FileWriter fw = new FileWriter(file.getAbsoluteFile(),true);
			BufferedWriter bw = new BufferedWriter(fw);

			bw.write("the exit time is logged here: "+content);
			bw.write("the free memory is logged here when it exited: "+freeMemory);
			bw.close();

			

		} catch (Exception e) {
			e.printStackTrace();
		}
		//if so close this container:
		return;
    }
		JSONObject json = (JSONObject) instance.parser.parse(input);
		//PrintWriter out = new PrintWriter(System.out);
		String doc_id= (String)json.get("doc_id");
		//System.out.println(doc_id);
		String article_body=(String) json.get("article_body");
		//System.out.println(article_body);
		Annotation annotation = new Annotation(article_body);
		annotation.set(CoreAnnotations.DocIDAnnotation.class, doc_id);
		instance.annotate_list.add(annotation);
		if(instance.annotate_list.size()>=num_docs || instance.parse_timer.elapsed(TimeUnit.SECONDS)>=60000)
		{
			
			//System.out.println("Number of threads: "+num_proc);
			//System.out.println("parse time:"+instance.parse_timer);
			instance.pipeline.annotate(instance.annotate_list,num_proc , new Consumer<Annotation>() {

				public void accept(Annotation arg0) 
				{
					instance.doc_timer.reset();
					instance.doc_timer.start();
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
					//System.out.println("DOC_ID:"+instance.doc_timer);
					
				}
			});
			//System.out.println("Batch size:"+instance.annotate_list.size()+" docs");
			instance.log.debug(log_token+" Batch time:"+instance.batch_timer);
			//System.out.println("Batch time:"+instance.batch_timer);
			instance.batch_timer.reset();
			instance.batch_timer.start();
			instance.log.debug(log_token+" Total time:"+instance.total_timer);
			//System.out.println("Total time:"+instance.total_timer);
			instance.parse_timer.reset();
			instance.parse_timer.start();
			instance.annotate_list.clear();	 
			StanfordCoreNLP.clearAnnotatorPool();
		}
	}
}

