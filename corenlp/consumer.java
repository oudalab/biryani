import java.io.FileReader;
import java.io.IOException;
import java.net.ConnectException;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.spi.LoggerFactory;
import org.bson.Document;
import org.hamcrest.core.IsNot;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.google.common.base.Stopwatch;
import com.mongodb.Block;
import com.mongodb.BulkUpdateRequestBuilder;
import com.mongodb.BulkWriteOperation;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
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
import edu.stanford.nlp.semgraph.SemanticGraph;
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations.CollapsedCCProcessedDependenciesAnnotation;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.trees.TreeCoreAnnotations.TreeAnnotation;
import edu.stanford.nlp.util.CoreMap;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.time.LocalDateTime;
import java.io.PrintStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
public class consumer 
{

	private static final consumer instance;
	private StanfordCoreNLP pipeline = null;
	private JSONParser parser = new JSONParser();
	//need to find a way to change this 500.
	private ArrayBlockingQueue<Annotation> queue;
	private ArrayBlockingQueue<Envelope> env_queue;
	private Stopwatch batch_timer;
	private Stopwatch total_timer;
	private Stopwatch idle_timer;
	private Stopwatch flush_timer;
	private Logger log=Logger.getLogger(getClass());
	private int docs_parsed;
	private int docs_inserted;
	private Channel channel=null;
	private java.sql.Connection c=null;
	static 
	{
		instance = new consumer();
		Properties props = new Properties();
		props.setProperty("annotators", "tokenize, ssplit,pos,parse");
		Integer cores= Runtime.getRuntime().availableProcessors();
		props.setProperty("threads", cores.toString());
		//props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner, parse, dcoref");
		props.setProperty("parse.model","edu/stanford/nlp/models/srparser/englishSR.ser.gz");
		instance.pipeline = new StanfordCoreNLP(props);
		instance.batch_timer= Stopwatch.createUnstarted();
		instance.total_timer=Stopwatch.createStarted();
		instance.idle_timer=Stopwatch.createStarted();
		instance.flush_timer=Stopwatch.createStarted();
		instance.docs_parsed=0;
		instance.docs_inserted=0;
		try {
			Class.forName("org.sqlite.JDBC");
			instance.c = DriverManager.getConnection("jdbc:sqlite:test.db");
			PreparedStatement stmt = instance.c.prepareStatement("CREATE TABLE IF NOT EXISTS json_test_table (id VARCHAR PRIMARY KEY ,output VARCHAR)");
            stmt.executeUpdate();
		} catch ( Exception e ) {
			System.err.println( e.getClass().getName() + ": " + e.getMessage() );
			 
		}
		System.out.println("Opened database successfully");
		
	}

	public static void main(String[] argv) throws Exception 
	{
		//BasicConfigurator.configure();
		PropertyConfigurator.configure("log4j.properties");

		final int num_proc ;
		final int num_docs;
		final String log_token;
		if (argv.length==1)
		{
			num_proc=Integer.parseInt(argv[0]);
			num_docs=1;
			log_token="test";

		}
		else if(argv.length==2)
		{
			num_proc=Integer.parseInt(argv[0]);
			num_docs=Integer.parseInt(argv[1]);
			log_token="test";
		}
		else if(argv.length==3)
		{
			num_proc=Integer.parseInt(argv[0]);
			num_docs=Integer.parseInt(argv[1]);
			log_token=argv[2];

		}
		else
		{
			num_proc=1;
			num_docs=1;
			log_token="test";
		}
		System.out.println("Batch size: "+num_docs);
		System.out.println("#threads: "+num_proc);
		String R_ip = "";
		int R_port = 0;
		String R_usr = "";
		String R_pass = "";
		String R_vhost = "";
		String R_queue = "";
		String M_ip= "";
		int M_port= 0;
		String M_db= "";
		instance.queue=new ArrayBlockingQueue<Annotation>(num_docs);
		instance.env_queue= new ArrayBlockingQueue<Envelope>(num_docs);

		Thread monitorThread= new Thread() {
			public void run() {
				try {
					while(true)
						//sleep 10 secs then check memory;
					{
						Thread.sleep(10000);
						int mb=1024*1024;
						Runtime instanceRuntime=Runtime.getRuntime();
						instance.log.debug(log_token+" MEMORY LOG:"+" Total Memory:"+instanceRuntime.totalMemory() / mb);
						instance.log.debug(log_token+" MEMORY LOG:"+" Free Memory:"+instanceRuntime.freeMemory() / mb);
						instance.log.debug(log_token+" MEMORY LOG:"+" Used Memory:"+(instanceRuntime.totalMemory()-instanceRuntime.freeMemory())/mb);
						instance.log.debug(log_token+" MEMORY LOG:"+" Max Memory: "+instanceRuntime.maxMemory()/mb);
					}
				} 
				catch(InterruptedException v) 
				{
					System.out.println(v);
				}
			}  
		};
		//start the monitor thread here.
		//monitorThread.start();
		
		Timer timer= new Timer();
		timer.schedule(new TimerTask() {
			
			@Override
			public void run() {
				// TODO Auto-generated method stub
				//System.out.println("Timer method Queue Size "+instance.queue.size());
				int last_index= instance.env_queue.size();
				ArrayList<Envelope>envArrayList= new ArrayList<Envelope>();
				instance.env_queue.drainTo(envArrayList);
				if(last_index>0 && instance.flush_timer.elapsed(TimeUnit.MILLISECONDS)>=60000)
				{
					try 
					{
						doWork(num_proc, envArrayList.size(), log_token, true);
						instance.channel.basicAck(envArrayList.get(last_index-1).getDeliveryTag(), true);
					} 	
					catch (IOException e) 
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					catch(Exception e1)
					{
						e1.printStackTrace();
					}
				}
			}
		}, 0,60000*2);
		//**************************************************************//
		
		

		try 
		{
			FileReader reader = new FileReader("corenlp.json");
			JSONObject jsonobject = (JSONObject) new JSONParser().parse(reader);
			JSONObject rabbit = (JSONObject) jsonobject.get("rabbitmq");
			JSONObject mongo = (JSONObject) jsonobject.get("mongodb");
			R_ip = (String) rabbit.get("ip");
			R_port = Integer.parseInt((String) rabbit.get("port"));
			R_usr = (String) rabbit.get("username");
			R_pass = (String) rabbit.get("password");
			R_vhost = (String) rabbit.get("vhost");
			R_queue = (String) rabbit.get("queue");
			M_ip= (String) mongo.get("ip");
			M_port= Integer.parseInt((String) mongo.get("port"));
			M_db= (String) mongo.get("db");
			 
		} 
		catch (Exception e) 
		{
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
			instance.channel = connection.createChannel();
			instance.channel.queueDeclare(R_queue, true, false, false, null);
			instance.channel.basicQos(num_docs);
			
			DefaultConsumer consumer_rabbimq = new DefaultConsumer(instance.channel) 
			{
				//create this varaible to do batch acknowledgement
				int ackCount=0;
				@Override
				public void handleDelivery(String consumerTag, Envelope envelope,
						AMQP.BasicProperties properties, byte[] body) throws IOException 
				{
					ackCount=ackCount+1;
					String message = new String(body, "UTF-8");
					// System.out.println(" [x] Received  messages'");
					try 
					{ 
						JSONObject json = (JSONObject) instance.parser.parse(message);
						String doc_id= (String)json.get("doc_id");
						String article_body=(String) json.get("article_body");
						Annotation annotation = new Annotation(article_body);
						annotation.set(CoreAnnotations.DocIDAnnotation.class, doc_id);
						instance.queue.put(annotation);
						instance.env_queue.put(envelope);
						instance.flush_timer.reset();
						instance.flush_timer.start();
						doWork(num_proc,num_docs,log_token,false);
					}
					//exit error may not be a parser exception 
					catch (Exception e) 
					{
						// TODO Auto-generated catch block
						//e.printStackTrace();
						try 
						{
							//in case it can not write the same file at the same time
							File file = new File(log_token+"_"+".exitlog");

							// if file doesnt exists, then create it
							if (!file.exists()) {
								file.createNewFile();
							}
							//he true will make sure it is being append
							FileWriter fw = new FileWriter(file.getAbsoluteFile(),true);

							PrintStream ps=new PrintStream(file);
							e.printStackTrace(ps);
							ps.close();

						} catch (IOException e1) {
							System.out.println("this is error for logging the memory leakage: ");
							e1.printStackTrace();
						}

					} 	
					finally 
					{ //set the multiple acknoledge to be true, and only ackownledge every 500 docs
						//ToDo: need a way to ackowledge only when the 500 docs finish parsing, make it more durable.
						if(ackCount%num_docs==0)
						{
							instance.channel.basicAck(envelope.getDeliveryTag(), true);
						}
					}
				}
			};
			instance.channel.basicConsume(R_queue, false, consumer_rabbimq);
		}catch(ConnectException e2)
		{
			e2.printStackTrace();
			instance.log.error("Cannot connect to server, network error!");
		}
		finally
		{

		}
	}
	private static void doWork(int num_proc,int num_docs,final String log_token, boolean flush) throws Exception {
		//System.out.println("Inside dowork queue size"+instance.queue.size());
		if(instance.queue.remainingCapacity()==0 || flush==true)
		{
			if(flush)
				instance.log.debug(log_token+" Timeout Parsing");
			instance.env_queue.clear();
			String id= UUID.randomUUID().toString();
			instance.log.debug(log_token+": "+id+" Standord Thread started");
			instance.batch_timer.start();
			ArrayList<Annotation> tempArraylist= new ArrayList<Annotation>();
			instance.queue.drainTo(tempArraylist);
			if(tempArraylist.size()>0)
			{
				//System.out.println(instance.pipeline.getProperties());
				instance.pipeline.annotate(tempArraylist,num_proc , new Consumer<Annotation>() {

					public void accept(Annotation arg0) 
					{
						instance.docs_parsed++;
						String doc_id= arg0.get(CoreAnnotations.DocIDAnnotation.class);
						JSONObject doc_out= new JSONObject(); // main object
						doc_out.put("doc_id", doc_id);
						JSONArray sen_array= new JSONArray();
						instance.log.debug(doc_id+": PARSING");
						List<CoreMap> sentences = arg0.get(SentencesAnnotation.class);
						int sen_id=0;
						for(CoreMap sentence: sentences) 
						{
							/*for (CoreLabel token: sentence.get(TokensAnnotation.class)) 
							{
								String word = token.get(TextAnnotation.class);
								String pos = token.get(PartOfSpeechAnnotation.class);
								String ne = token.get(NamedEntityTagAnnotation.class);
							}*/
							Tree tree = sentence.get(TreeAnnotation.class);
							JSONObject sen_obj= new JSONObject(); // sentence object;
							sen_obj.put("sen_id", ++sen_id);
							sen_obj.put("sentence", sentence);
							sen_obj.put("tree", tree);
							sen_array.add(sen_obj);
							//SemanticGraph dependencies = sentence.get(CollapsedCCProcessedDependenciesAnnotation.class);
						}
						instance.log.debug(doc_id+": PARSED");
						doc_out.put("sentences", sen_array);
						//System.out.println(doc_out);
						try {
							PreparedStatement stmt = instance.c.prepareStatement("INSERT INTO json_test_table (id, output) VALUES (?,?)");
							stmt.setString(1, doc_id.toString());
							stmt.setString(2, doc_out.toJSONString());
							if(stmt.executeUpdate()==1){
								instance.log.debug(log_token+": "+ ++instance.docs_inserted+": Successfully inserted");
							}
							 
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				});

			}
			//instance.log.debug(log_token+" #Documents: "+instance.docs_parsed);
			//String Stanford_timing=instance.pipeline.timingInformation();
			//instance.log.debug("Stanford Timing: "+Stanford_timing);
			instance.log.debug(log_token+" #Batch_Doc:" +num_docs +" Batch time: "+instance.batch_timer);
			instance.batch_timer.reset();
			instance.log.debug(log_token+" #Documents:" +instance.docs_parsed +" Total time: "+instance.total_timer);
			instance.idle_timer.reset();
			instance.idle_timer.start();
			tempArraylist.clear();	 
			instance.log.debug(log_token+": "+id+" Stanford Thread completed");
		}
	}
}
