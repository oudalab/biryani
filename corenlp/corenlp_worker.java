import com.google.common.base.Stopwatch;
import com.rabbitmq.client.*;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.trees.TreeCoreAnnotations.TreeAnnotation;
import edu.stanford.nlp.util.CoreMap;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class corenlp_worker
{
	private final static corenlp_worker instance;
	private Logger log=Logger.getLogger(getClass());
	private String TASK_QUEUE_NAME;
	private Properties props = new Properties();
    private StanfordCoreNLP corenlp_pipeline ;
    private java.sql.Connection c;
    private int batch_size;
    private int threads;
    private String db_name;
    private Integer cores;
    private String restart_status="empty";
    private int total_docs_processed;
    private ArrayList<String> mongo_array_list = new ArrayList<String>();
	private int restart_doc_count =0;
    private String log_token;
    private Timer timer = new Timer();
    private boolean is_pipeline_active=  false;
    private Channel channel;
    private Envelope envelope;
    private Stopwatch batch_timer = Stopwatch.createUnstarted();
    private Stopwatch total_timer = Stopwatch.createStarted();
    private int previous_batch_time = 0;
    private String previous_processed_doc= "previous";
    private String current_processed_doc = "current"; 
    private stats stats = new stats("stats");
    private int sents_parsed = 0;
    private int batch_data_bytes =0;
    private int io_operation =0;
	private ArrayList<Annotation> annotation_documents_list = new ArrayList<Annotation>();
	
	static 
	{
		instance = new corenlp_worker();
		instance.props.setProperty("annotators", "tokenize, ssplit,pos,parse");
	    instance.cores= Runtime.getRuntime().availableProcessors();
	    instance.props.setProperty("threads", instance.cores.toString());
	    instance.props.setProperty("parse.model","edu/stanford/nlp/models/srparser/englishSR.ser.gz");
	    instance.corenlp_pipeline = new StanfordCoreNLP(instance.props);
	    instance.total_docs_processed =0;
	}

	public static void main(String[] argv)
	{
		if (argv.length==1)
        {
            instance.threads=Integer.parseInt(argv[0]);
            instance.batch_size=1;
            instance.log_token="test";
            instance.db_name="test";

        }
        else if(argv.length==2)
        {
            instance.threads=Integer.parseInt(argv[0]);
            instance.batch_size=Integer.parseInt(argv[1]);
            instance.log_token="test";
            instance.db_name="test";
        }
        else if(argv.length==3)
        {
            instance.threads=Integer.parseInt(argv[0]);
            instance.batch_size=Integer.parseInt(argv[1]);
            instance.log_token=argv[2];
            instance.db_name="test";

        }
        else if(argv.length==4)
        {
        	instance.threads=Integer.parseInt(argv[0]);
            instance.batch_size=Integer.parseInt(argv[1]);
            instance.log_token=argv[2];
            instance.db_name=argv[3];
        }
        else
        {
            instance.threads=instance.cores;
            instance.batch_size=1;
            instance.log_token="test";
            instance.db_name="test";
        }
		instance.timer.schedule(new TimerTask() {
			
			@Override
			public void run() 
			{
				System.out.println(instance.previous_processed_doc);
				System.out.println(instance.current_processed_doc);
				System.out.println((int)instance.batch_timer.elapsed(TimeUnit.SECONDS));
				System.out.println("Pipeline status:"+instance.is_pipeline_active);
				if(instance.previous_batch_time>0 && instance.is_pipeline_active)
				{
					System.out.println(instance.previous_batch_time);
					if((int)instance.batch_timer.elapsed(TimeUnit.SECONDS) > 2*instance.previous_batch_time)
					{
						instance.log.debug(instance.log_token+" "+"Taking So long to Process! Restating the container");
						System.exit(1);
					}
				}
				
				if(!instance.is_pipeline_active && instance.annotation_documents_list.size()>0)
				{
					//System.out.println("Process remaining documents");
					doWork(instance.annotation_documents_list, instance.threads);
					try 
					{
						instance.log.debug(instance.log_token+" "+"Processing remaining Documents");
						instance.channel.basicAck(instance.envelope.getDeliveryTag(), true);
					} 
					catch (IOException e)
					{
						instance.log.error(getExceptionSting(e));
					}
				}
				if(instance.previous_processed_doc.equals(instance.current_processed_doc)
						&& instance.is_pipeline_active)
				{
					try 
					{
						instance.log.debug(instance.log_token+" "+"Pipeline is stuck! Restarting container");
						instance.channel.basicAck(instance.envelope.getDeliveryTag(),true);
						instance.log.debug("Stuck pipeline... Auto ACK Messages");
					} 
					catch (IOException e) 
					{
						// TODO Auto-generated catch block
						instance.log.error(getExceptionSting(e));
					}
					System.exit(1);
				}
				else
				{
					instance.previous_processed_doc = instance.current_processed_doc;
				}
				
			}
		},0, 60000);
		
		/* Confguring logger */
		PropertyConfigurator.configure("log4j.properties");

		
		try
        {
            FileReader reader = new FileReader("corenlp.json");
            JSONObject jsonobject = (JSONObject) new JSONParser().parse(reader);
            JSONObject rabbit = (JSONObject) jsonobject.get("rabbitmq");
             
             
            String rabbitmq_ip = (String) rabbit.get("ip");
            int rabbitmq_port = Integer.parseInt((String) rabbit.get("port"));
            String rabbitmq_username = (String) rabbit.get("username");
            String rabbitmq_password = (String) rabbit.get("password");
            String rabbitmq_vhost = (String) rabbit.get("vhost");
            
            instance.TASK_QUEUE_NAME = (String) rabbit.get("queue");
            
            Class.forName("org.sqlite.JDBC");
            instance.c = DriverManager.getConnection("jdbc:sqlite:"+instance.db_name+".db");
            PreparedStatement stmt = instance.c.prepareStatement("CREATE TABLE IF NOT EXISTS json_test_table (id VARCHAR , date VARCHAR, output VARCHAR, mongo_id VARCHAR)");
            stmt.executeUpdate();
            
            /* Create a file or read to know the restart status of the file */
            File restart_file= new File("restart_status.txt");
            if(!restart_file.exists())
            {
                restart_file.createNewFile();
                FileWriter fw= new FileWriter(restart_file.getAbsoluteFile());
                BufferedWriter bw= new BufferedWriter(fw);
                bw.write(instance.log_token);
                bw.write(":started");
                bw.close();
            }
            else
            {
                BufferedReader br= new BufferedReader(new FileReader(restart_file.getAbsoluteFile()));
                String []data= br.readLine().split(":");
                instance.restart_status=data[1];
                br.close();
            }
            
            /* Creating Rabbitmq connection*/
    	    ConnectionFactory factory = new ConnectionFactory();
    	    factory.setAutomaticRecoveryEnabled(true);
    	    
    	    factory.setHost(rabbitmq_ip);
    	    factory.setPort(rabbitmq_port);
    	    factory.setUsername(rabbitmq_username);
    	    factory.setPassword(rabbitmq_password);
    	    factory.setVirtualHost(rabbitmq_vhost);
    	    factory.setConnectionTimeout(0);
    	    factory.setRequestedHeartbeat(600);
 
    	    final Connection connection = factory.newConnection();
    	    instance.channel = connection.createChannel();
    	    instance.channel.addShutdownListener(new ShutdownListener() {
				
				public void shutdownCompleted(ShutdownSignalException arg0) {
					// TODO Auto-generated method stub
					instance.log.error("Rabbitmq Connection Closed... Restarting the container");
					System.exit(1);
				}
			});

    	    instance.channel.queueDeclare(instance.TASK_QUEUE_NAME, true, false, false, null);
    	    //System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

    	    instance.channel.basicQos(2*instance.batch_size);

    	    final DefaultConsumer consumer = new DefaultConsumer(instance.channel) {
    	      
    	    	int ack_count=0;
    	      @Override
    	      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException 
    	      {  
    	        try 
    	        {
    	        	instance.envelope = envelope;
    	        	String message = new String(body, "UTF-8");
        	        instance.restart_doc_count++;
        	        
        	        document document = new document(message);
        	        String doc_id = document.getDoc_id();
        	        String article_body= document.getArticle_body();
        	        String publication_date = document.getPubclication_date();
        	        String mongo_id = document.getMongo_id();
        	        
        	        instance.batch_data_bytes+=article_body.getBytes("UTF-8").length;
        	        
        	        Annotation annotation = new Annotation(article_body);
                    annotation.set(CoreAnnotations.DocIDAnnotation.class,doc_id);
                    annotation.set(CoreAnnotations.DocDateAnnotation.class,publication_date);
                    annotation.set(CoreAnnotations.DocTitleAnnotation.class,mongo_id);
                    
                    if(instance.restart_doc_count>=instance.batch_size)
                        instance.restart_status="empty";
                    
                    if(instance.restart_status.equals("started") && instance.restart_doc_count<=instance.batch_size)
                    {
                        if(instance.mongo_array_list.size()<=0)
                        {
                            instance.log.debug(instance.log_token+" Container restarted and fetching documents");
                            instance.mongo_array_list=new sqlite_reader().doc_present(instance.db_name,instance.batch_size);
                        }
                        if(!instance.mongo_array_list.contains(mongo_id))
                        {
                        	ack_count++;
                        	instance.annotation_documents_list.add(annotation);
                        }
                    }
                    else
                    {
                        ack_count++;
                        instance.annotation_documents_list.add(annotation);
                    }
    	        	 
    	        	if(instance.annotation_documents_list.size() == instance.batch_size)
    	        		doWork(instance.annotation_documents_list,instance.threads);
    	        } 
    	        finally 
    	        {
    	          if(ack_count % instance.batch_size == 0) 
    	          {
    	        	  //System.out.println("Sending Acknowledgement");
    	        	  instance.channel.basicAck(instance.envelope.getDeliveryTag(),true);
    	          }
    	        }
    	      }
    	    };
    	    
    	    boolean autoAck = false;
    	    instance.channel.basicConsume(instance.TASK_QUEUE_NAME, autoAck, consumer);
             

        }
		catch(TimeoutException TOE)
		{
			instance.log.error(getExceptionSting(TOE));
			TOE.printStackTrace();
		}
		catch (IOException IO)
		{
			instance.log.error(getExceptionSting(IO));
			IO.printStackTrace();
		}
		catch (ClassNotFoundException CNE) 
		{
			instance.log.error(instance.log_token+":"+getExceptionSting(CNE));
			CNE.printStackTrace();
		} catch (SQLException SQLE) 
		{
			instance.log.error(instance.log_token+":"+getExceptionSting(SQLE));
			SQLE.printStackTrace();
		} catch (ParseException PE) 
		{
			instance.log.error(instance.log_token+":"+getExceptionSting(PE));
			PE.printStackTrace();
		}
		 
	  }

	  private static void doWork(ArrayList<Annotation> annotaion_documents_list, int num_threads) 
	  {
		  int num_docs = annotaion_documents_list.size();
		  System.out.println(instance.batch_size+" "+num_threads);
		  instance.log.debug(instance.log_token+" Started pipeline with #Threads:"+num_threads+" #Batch_size:"+instance.annotation_documents_list.size());
		  instance.is_pipeline_active = true;
		  instance.batch_timer.start();
		   instance.corenlp_pipeline.annotate(annotaion_documents_list,num_threads,new Consumer<Annotation>() {
			
			public void accept(Annotation arg0) {
				
                String doc_id= arg0.get(CoreAnnotations.DocIDAnnotation.class);
                String pub_date=arg0.get(CoreAnnotations.DocDateAnnotation.class);
                String mongo_id=arg0.get(CoreAnnotations.DocTitleAnnotation.class);
                
                instance.current_processed_doc = mongo_id;
                instance.log.debug(instance.log_token+" Processed:"+mongo_id);
               
                JSONObject doc_out= new JSONObject();; // main object
                doc_out.put("doc_id", doc_id);
                JSONArray sen_array= new JSONArray();
                 
                //System.out.println("Processing"+instance.current_processing_doc_id);
                instance.log.debug(doc_id+": PARSING");
                List<CoreMap> sentences = arg0.get(SentencesAnnotation.class);
                Integer sen_id=0;
                instance.sents_parsed+=sentences.size();
                 
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
                try 
                {
                    PreparedStatement stmt = instance.c.prepareStatement("INSERT INTO json_test_table (id,date,output,mongo_id) VALUES (?,?,?,?)");
                    stmt.setString(1, doc_id.toString());
                    stmt.setString(2, pub_date.toString());
                    stmt.setString(3, doc_out.toJSONString());
                    stmt.setString(4, mongo_id);
                    
                    if(stmt.executeUpdate()==1)
                    {
                    	instance.io_operation++;
                    	instance.total_docs_processed++;
                        instance.log.debug(instance.log_token+":"+instance.total_docs_processed+" Inserted");
                    }
                    else
                    {
                        instance.log.error(instance.log_token+"ERROR in inserting document");
                    }

                } 
                catch (SQLException e) {
                    e.printStackTrace();
                    instance.log.error(instance.log_token+"Exception in inserting documents");
                }	
			}
		});
		 instance.stats.insert_data("stats", String.valueOf(instance.threads), String.valueOf(num_docs), String.valueOf(instance.batch_data_bytes), String.valueOf(instance.sents_parsed), String.valueOf(instance.io_operation),String.valueOf(instance.batch_timer.elapsed(TimeUnit.SECONDS)));
		 instance.log.debug(instance.log_token+" #Documents:"+instance.total_docs_processed+":Processed::Time:"+instance.total_timer);
		 instance.annotation_documents_list.clear();
		 instance.sents_parsed=0;
		 instance.batch_data_bytes=0;
		 instance.io_operation=0;
		 instance.previous_batch_time = (int) instance.batch_timer.elapsed(TimeUnit.SECONDS);
		 instance.batch_timer.reset();
		 
		 instance.is_pipeline_active = false;
		 if(instance.total_docs_processed%num_docs != 0)
		 {
			 instance.log.debug(instance.log_token+":Document is stuck... Restart container");
			 System.exit(1);
		 } 
	  }
	  
	  public static String getExceptionSting(Exception e)
	    {
	    	StringWriter errors = new StringWriter();
	    	e.printStackTrace(new PrintWriter(errors));
	    	return errors.toString();
	    }
}


