import com.google.common.base.Stopwatch;
import com.rabbitmq.client.*;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.trees.TreeCoreAnnotations.TreeAnnotation;
import edu.stanford.nlp.util.CoreMap;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import java.io.*;
import java.net.ConnectException;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class consumer
{

	private static final consumer instance;
	private StanfordCoreNLP pipeline = null;
	private JSONParser parser = new JSONParser();
	private ArrayBlockingQueue<Annotation> queue;
	private ArrayBlockingQueue<Envelope> env_queue;
	private Stopwatch batch_timer;
	private Stopwatch total_timer;
	private Stopwatch flush_timer;
	private Logger log=Logger.getLogger(getClass());
	private int batch_size;
	private int threads;
	private int docs_parsed;
	private int docs_inserted;
	private Channel channel=null;
	private java.sql.Connection c=null;
	private static String restart_status="empty";
	private int restart_doc_count;
	private ArrayList<String> mongoArrayList;
	private double cpu_usage;
	private double used_memory;
	private double cpu_refer;
	private double mem_refer;
	private int cpu_refer_count;
	private int mem_refer_count;
	private double main_avg_used_memory;
	private double main_avg_cpu_usage;
	private int main_avg_count;
	private Sigar sigar;
	private int change_batch_count;
	private String R_queue;
	private DefaultConsumer consumer_rabbimq;
	private Long prev_batch_time;
	private String change_batch;
	private Long expected_batch_time;
	private int batch_variator;
	private int optimal_batch;
	private int exptected_time_raiser;
	private int optimal_batch_test;
	private int initial_batch_size;
	private int initial_optimal_batch_test;
	private String consumer_tag;
	private boolean initial_start;

	static
	{
		instance = new consumer();
		instance.batch_size=0;
		instance.threads=0;
		Properties props = new Properties();
		props.setProperty("annotators", "tokenize, ssplit,pos,parse");
		props.setProperty("parse.model","edu/stanford/nlp/models/srparser/englishSR.ser.gz");
		instance.pipeline = new StanfordCoreNLP(props);
		instance.batch_timer= Stopwatch.createUnstarted();
		instance.total_timer=Stopwatch.createStarted();
		Stopwatch.createStarted();
		instance.flush_timer=Stopwatch.createStarted();
		instance.docs_parsed=0;
		instance.docs_inserted=0;
		instance.restart_doc_count=0;
		instance.mongoArrayList=new ArrayList<String>();
		instance.cpu_usage=0;
		instance.used_memory=0;
		instance.cpu_refer=0;
		instance.mem_refer=0;
		instance.cpu_refer_count=0;
		instance.mem_refer_count=0;
		instance.main_avg_cpu_usage=0;
		instance.main_avg_used_memory=0;
		instance.change_batch="NO";
		instance.change_batch_count=1;
		instance.main_avg_count=instance.change_batch_count;
		instance.prev_batch_time= Long.valueOf(5*60);
		instance.expected_batch_time=instance.prev_batch_time;
		instance.sigar=new Sigar();
		instance.batch_variator=250;
		instance.optimal_batch=0;
		instance.exptected_time_raiser=5;
		instance.optimal_batch_test=0;
		instance.initial_batch_size=0;
		instance.initial_optimal_batch_test=0;
		instance.consumer_tag=UUID.randomUUID().toString();
		instance.initial_start=true;

		try {
			Class.forName("org.sqlite.JDBC");
			instance.c = DriverManager.getConnection("jdbc:sqlite:test.db");
			PreparedStatement stmt = instance.c.prepareStatement("CREATE TABLE IF NOT EXISTS json_test_table (id VARCHAR , date VARCHAR, output VARCHAR, mongo_id VARCHAR)");
			stmt.executeUpdate();
		} catch ( Exception e ) {
			System.err.println( e.getClass().getName() + ": " + e.getMessage() );
			instance.log.debug("Error with SQlite");

		}
		instance.log.debug("Database successfully connected");


	}

	public static void main(String[] argv) throws Exception
	{
		PropertyConfigurator.configure("log4j.properties");
		final String log_token;
		if (argv.length==1)
		{
			instance.threads=Integer.parseInt(argv[0]);
			instance.batch_size=1;
			log_token="test";

		}
		else if(argv.length==2)
		{
			instance.threads=Integer.parseInt(argv[0]);
			instance.batch_size=Integer.parseInt(argv[1]);
			log_token="test";
		}
		else if(argv.length==3)
		{
			instance.threads=Integer.parseInt(argv[0]);
			instance.batch_size=Integer.parseInt(argv[1]);
			log_token=argv[2];

		}
		else
		{
			instance.threads=1;
			instance.batch_size=1;
			log_token="test";
		}
		instance.initial_batch_size=instance.batch_size;
		File restart_file= new File("restart_status.txt");
		if(!restart_file.exists())
		{
			restartFileWriteData("restart_status.txt","started",instance.batch_size ,log_token,instance.prev_batch_time);
		}
		else
		{
			String file_data[]=getRestartFileData("restart_status.txt");
			restart_status=(file_data[0].split(":"))[1];
			instance.batch_size= Integer.parseInt((file_data[1].split(":"))[1]);
			instance.prev_batch_time= Long.valueOf(file_data[2].split(":")[1]);
			instance.expected_batch_time=instance.prev_batch_time;
			instance.initial_batch_size=instance.batch_size;
			System.out.println(instance.expected_batch_time);
		}
		instance.log.debug(log_token+" #Threads: "+instance.threads+" #Batch_size: "+instance.batch_size);

		String R_ip = "";
		int R_port = 0;
		String R_usr = "";
		String R_pass = "";
		String R_vhost = "";
		String M_ip= "";
		int M_port= 0;
		String M_db= "";
		instance.queue=new ArrayBlockingQueue<Annotation>(instance.batch_size);
		instance.env_queue= new ArrayBlockingQueue<Envelope>(instance.batch_size);
		Timer timer= new Timer();
		timer.schedule(new TimerTask() {
			int last_index=0;
			ArrayList<Envelope>envArrayList= new ArrayList<Envelope>();
			@Override
			public void run() {
				instance.env_queue.drainTo(envArrayList);
				last_index= envArrayList.size();
				if(!instance.batch_timer.isRunning() && last_index>0 && instance.flush_timer.elapsed(TimeUnit.MILLISECONDS)>=60000)
				{
					try
					{
						doWork(instance.threads, envArrayList.size(), log_token, true);
						instance.channel.basicAck(envArrayList.get(last_index-1).getDeliveryTag(), true);
						last_index=0;
						envArrayList.clear();
					}
					catch (IOException e)
					{
						e.printStackTrace();
					}
					catch(Exception e1)
					{
						e1.printStackTrace();
					}
				}
			}
		}, 0,60000);
		Timer batch_timer_thread= new Timer();
		batch_timer_thread.schedule(new TimerTask() {
			@Override
			public void run() {
				if(instance.batch_timer.isRunning() && !instance.initial_start)
				{
					System.out.println("Batch timer:"+instance.batch_timer);
					if(instance.batch_timer.elapsed(TimeUnit.SECONDS)>instance.prev_batch_time*2)
					{
						instance.log.error(log_token+"Taking too long for batch to execute...Restarting the container ");
						System.exit(1);
					}

				}
			}
		}, 0,60000);
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
			instance.R_queue = (String) rabbit.get("queue");
			M_ip= (String) mongo.get("ip");
			M_port= Integer.parseInt((String) mongo.get("port"));
			M_db= (String) mongo.get("db");

		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
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
			instance.channel.queueDeclare(instance.R_queue, true, false, false, null);
			instance.channel.basicQos(3*instance.batch_size+instance.batch_variator);
			instance.consumer_rabbimq = new DefaultConsumer(instance.channel)
			{
				int ackCount=0;
				public void handleDelivery(String consumerTag, Envelope envelope,
						AMQP.BasicProperties properties, byte[] body) throws IOException
						{
					String message = new String(body, "UTF-8");
					try
					{
						instance.restart_doc_count++;
						JSONObject json = (JSONObject) instance.parser.parse(message);
						String doc_id= (String)json.get("doc_id");
						String article_body=(String) json.get("article_body");
						String pub_date=(String) json.get("publication_date_raw");
						String mongo_id_json_str=json.get("_id").toString();
						JSONObject mongo_id_json_obj= (JSONObject) instance.parser.parse(mongo_id_json_str);
						String mongo_id= (String) mongo_id_json_obj.get("$oid");
						Annotation annotation = new Annotation(article_body);
						annotation.set(CoreAnnotations.DocIDAnnotation.class, doc_id);
						annotation.set(CoreAnnotations.DocDateAnnotation.class, pub_date);
						annotation.set(CoreAnnotations.DocTitleAnnotation.class,mongo_id);
						if(instance.restart_doc_count>=instance.batch_size)
							restart_status="empty";
						if(restart_status.equals("started") && instance.restart_doc_count<=instance.batch_size)
						{
							if(instance.mongoArrayList.size()<=0)
							{
								instance.log.debug(log_token+" Container restarted and fetching documents");
								instance.mongoArrayList=new sqlite_reader().doc_present(instance.batch_size);
							}
							if(!instance.mongoArrayList.contains(mongo_id))
							{
								ackCount=ackCount+1;
								instance.queue.put(annotation);
								instance.env_queue.put(envelope);
								instance.flush_timer.reset();
								instance.flush_timer.start();
							}
						}
						else
						{
							ackCount=ackCount+1;
							instance.queue.put(annotation);
							instance.env_queue.put(envelope);
							instance.flush_timer.reset();
							instance.flush_timer.start();
						}
						doWork(instance.threads,instance.batch_size,log_token,false);
					}
					catch (Exception e)
					{
						e.printStackTrace();
					}
					finally
					{  
						if(ackCount==instance.batch_size)
						{ 
							instance.initial_start=false;
							instance.channel.basicAck(envelope.getDeliveryTag(), true);
							if(!instance.change_batch.equals("NO"))
							{
								if(instance.change_batch.equals("I"))
								{

									if(instance.batch_size+instance.batch_variator<=instance.optimal_batch || instance.optimal_batch==0 || instance.optimal_batch==instance.initial_batch_size)
									{
										//System.out.println("Increase Batch count");
										instance.batch_size=instance.batch_size+instance.batch_variator;
										instance.log.debug(log_token+" Increased Batch size: "+instance.batch_size);
									}
								}
								if(instance.change_batch.equals("D"))
								{

									//System.out.println("Decrease Batch count");
									instance.batch_size=instance.batch_size-instance.batch_variator;
									instance.log.debug(log_token+" Decreased Batch size: "+instance.batch_size);
								}
								instance.change_batch="NO";
								//System.out.println("Changing batch: "+instance.batch_size);
								instance.log.debug(log_token+": Changing batch: "+instance.batch_size);
								restartFileWriteData("restart_status.txt","started",instance.batch_size ,log_token,instance.expected_batch_time);
								instance.consumer_rabbimq.getChannel().basicQos(3*instance.batch_size+instance.batch_variator);
								instance.queue=new ArrayBlockingQueue<Annotation>(instance.batch_size);
								instance.env_queue= new ArrayBlockingQueue<Envelope>(instance.batch_size);
								instance.channel.basicCancel(instance.consumer_tag);
								System.out.println(instance.consumer_tag+": Cancelled");
								instance.consumer_tag=UUID.randomUUID().toString();
								instance.channel.basicConsume(instance.R_queue, false,instance.consumer_tag,instance.consumer_rabbimq);
								System.out.println(instance.consumer_tag+": started");
							}
							ackCount=0;
						}
					}
						}
			};
			if(instance.initial_start)
			{
				instance.channel.basicConsume(instance.R_queue, false,instance.consumer_tag, instance.consumer_rabbimq);
				System.out.println(instance.consumer_tag+":Started");
				//instance.initial_start=false;
			}

		}catch(ConnectException e2)
		{
			e2.printStackTrace();
			instance.log.error("Cannot connect to server, network error!");
			System.out.println("Rabbitmq Error");
			System.exit(2);
		}
		finally
		{

		}
	}
	private static void doWork(int num_proc,int num_docs,final String log_token,boolean flush) throws Exception {
		if(instance.queue.remainingCapacity()==0 || flush==true)
		{
			System.out.println("#Batch: "+num_docs+"\t#Threads: "+num_proc);
			instance.log.debug(log_token+"#Batch: "+num_docs+"\t#Threads: "+num_proc );
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
				synchronized (instance)
				{
					instance.pipeline.annotate(tempArraylist,num_proc , new Consumer<Annotation>() {

						public void accept(Annotation arg0)
						{
							try {

								instance.mem_refer=instance.sigar.getMem().getUsedPercent();
								instance.cpu_refer=instance.sigar.getCpuPerc().getCombined();
								if(!Double.isNaN(instance.cpu_refer))
								{
									instance.cpu_usage+=instance.cpu_refer*100;
									instance.cpu_refer_count++;
								}
								if(!Double.isNaN(instance.mem_refer))
								{
									instance.used_memory+=instance.mem_refer;
									instance.mem_refer_count++;
								}

							} catch (SigarException e1) {
								e1.printStackTrace();
							}
							String doc_id= arg0.get(CoreAnnotations.DocIDAnnotation.class);
							String pub_date=arg0.get(CoreAnnotations.DocDateAnnotation.class);
							String mongo_id=arg0.get(CoreAnnotations.DocTitleAnnotation.class);
							JSONObject doc_out= new JSONObject(); // main object
							doc_out.put("doc_id", doc_id);
							JSONArray sen_array= new JSONArray();
							instance.log.debug(doc_id+": PARSING");
							List<CoreMap> sentences = arg0.get(SentencesAnnotation.class);
							Integer sen_id=0;
							for(CoreMap sentence: sentences)
							{
								Tree tree = sentence.get(TreeAnnotation.class);
								JSONObject sen_obj= new JSONObject(); // sentence object;
								sen_obj.put("sen_id", (++sen_id).toString());
								sen_obj.put("sentence", sentence.toString());
								sen_obj.put("tree", tree.toString());
								sen_array.add(sen_obj);
							}
							instance.log.debug(doc_id+": PARSED");
							doc_out.put("sentences", sen_array);
							try {
								PreparedStatement stmt = instance.c.prepareStatement("INSERT INTO json_test_table (id,date,output,mongo_id) VALUES (?,?,?,?)");
								stmt.setString(1, doc_id.toString());
								stmt.setString(2, pub_date.toString());
								stmt.setString(3, doc_out.toJSONString());
								stmt.setString(4, mongo_id);
								if(stmt.executeUpdate()==1){
									instance.log.debug(log_token+": "+ ++instance.docs_inserted+": Successfully inserted");
									instance.docs_parsed++;
								}
								else
								{
									instance.log.error(log_token+"ERROR in inserting document");
								}

							} catch (SQLException e) {
								e.printStackTrace();
								instance.log.error(log_token+"Exception in inserting documents");
							}
						}
					});
				}
			}
			instance.main_avg_cpu_usage+=instance.cpu_usage/instance.cpu_refer_count;
			instance.main_avg_used_memory+=instance.used_memory/instance.mem_refer_count;
			if(--instance.main_avg_count==0)
			{
				//System.out.println("Main Average CPU Used % : "+instance.main_avg_cpu_usage/instance.change_batch_count);
				//System.out.println("Main Average Memory Used % : "+instance.main_avg_used_memory/instance.change_batch_count);
				//System.out.println("Batch time:"+instance.batch_timer.elapsed(TimeUnit.SECONDS));
				instance.log.debug(log_token+":Main Average CPU Used % : "+instance.main_avg_cpu_usage/instance.change_batch_count);
				instance.log.debug(log_token+": Main Average Memory Used % : "+instance.main_avg_used_memory/instance.change_batch_count);
				if(instance.batch_timer.elapsed(TimeUnit.SECONDS)- instance.expected_batch_time > 2*instance.exptected_time_raiser)
				{
					int temp_batch= instance.batch_size-instance.batch_variator;
					if(temp_batch!=0)
					{
						instance.optimal_batch=temp_batch;
						instance.optimal_batch_test=0;
						//System.out.println("Optimal Batch size: "+instance.optimal_batch);
					}

				}
				if(instance.main_avg_cpu_usage/instance.change_batch_count<=95 && instance.main_avg_used_memory/instance.change_batch_count<=95 &&instance.batch_timer.elapsed(TimeUnit.SECONDS)<=instance.expected_batch_time+instance.exptected_time_raiser)
				{
					instance.change_batch="I";
					if(instance.optimal_batch==0)
						instance.expected_batch_time= instance.batch_timer.elapsed(TimeUnit.SECONDS)*(instance.batch_size+instance.batch_variator)/instance.batch_size;
					if(instance.optimal_batch>0)
						instance.expected_batch_time= instance.batch_timer.elapsed(TimeUnit.SECONDS)*instance.optimal_batch/instance.batch_size;
					//System.out.println("Expected Time for"+(instance.batch_size+instance.batch_variator)+": "+(instance.expected_batch_time+5));
				}
				else
				{
					if(instance.batch_size-instance.batch_variator>0)
					{
						instance.change_batch="D";
						if(instance.optimal_batch==0)
							instance.expected_batch_time= instance.batch_timer.elapsed(TimeUnit.SECONDS)*(instance.batch_size-instance.batch_variator)/instance.batch_size;
						if(instance.optimal_batch>0)
							instance.expected_batch_time= instance.batch_timer.elapsed(TimeUnit.SECONDS)*(instance.batch_size-instance.batch_variator)/instance.optimal_batch;
						//System.out.println("Expected Time for"+(instance.batch_size-instance.batch_variator)+": "+(instance.expected_batch_time+5));
					}
				}
				instance.main_avg_cpu_usage=0;
				instance.main_avg_used_memory=0;
				instance.main_avg_count=instance.change_batch_count;
			}
			instance.used_memory=0;
			instance.cpu_usage=0;
			instance.cpu_refer=0;
			instance.cpu_refer_count=0;
			instance.mem_refer=0;
			instance.mem_refer_count=0;
			instance.prev_batch_time=instance.batch_timer.elapsed(TimeUnit.SECONDS);
			instance.log.debug(log_token+" #Batch_Doc:" +instance.docs_parsed +" Batch time: "+instance.batch_timer);
			instance.batch_timer.reset();
			instance.log.debug(log_token+" #Documents:" +instance.docs_inserted+" Total time: "+instance.total_timer);
			instance.flush_timer.reset();
			instance.flush_timer.start();
			tempArraylist.clear();
			instance.docs_parsed=0;
			instance.log.debug(log_token+": "+id+" Stanford Thread completed");
		}
	}
	static void restartFileWriteData(String filename,String container_status,Integer latest_batch_count,String container_name,Long batch_time)
	{
		File restart_file= new File(filename);
		try {
			restart_file.createNewFile();
			FileWriter fw= new FileWriter(restart_file.getAbsoluteFile());
			BufferedWriter bw= new BufferedWriter(fw);
			bw.write(container_name);
			bw.write(":started;latest_batch:");
			bw.write(latest_batch_count.toString());
			bw.write(";previous_batch_time:");
			bw.write(batch_time.toString());
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	static String [] getRestartFileData(String filename)
	{
		File restart_file= new File(filename);
		BufferedReader br= null;
		String []data=null;
		try
		{
			br = new BufferedReader(new FileReader(restart_file.getAbsoluteFile()));
			data= br.readLine().split(";");

		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return data;
	}
}
