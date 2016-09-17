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
    private Stopwatch idle_timer;
    private Stopwatch flush_timer;
    
    private Logger log=Logger.getLogger(getClass());
    
    private int docs_parsed;
    private int docs_inserted;
    private Channel channel=null;
    private java.sql.Connection c=null;
    private static String restart_status="empty";
    private int restart_doc_count;
    private ArrayList<String> mongoArrayList;
    private int previous_time;
    private String db_name;
    private stats stats;
    private int io_operation;
    private int batch_data_bytes;
    private int sents_parsed;
    private String current_processing_doc_id;
    private String last_processed_doc_id;
    private Envelope envelope;
    
    static
    {
        instance = new consumer();
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit,pos,parse");
        Integer cores= Runtime.getRuntime().availableProcessors();
        props.setProperty("threads", cores.toString());
        props.setProperty("parse.model","edu/stanford/nlp/models/srparser/englishSR.ser.gz");
        instance.pipeline = new StanfordCoreNLP(props);
        instance.batch_timer= Stopwatch.createUnstarted();
        instance.total_timer=Stopwatch.createStarted();
        instance.idle_timer=Stopwatch.createStarted();
        instance.flush_timer=Stopwatch.createStarted();
        instance.docs_parsed=0;
        instance.docs_inserted=0;
        instance.restart_doc_count=0;
        instance.mongoArrayList=new ArrayList<String>();
        instance.previous_time=0;
        instance.db_name="test";
        instance.stats=new stats("stats");
        instance.io_operation=0;
        instance.batch_data_bytes=0;
        instance.sents_parsed=0;    
        instance.current_processing_doc_id ="current";
        instance.last_processed_doc_id ="last";
        instance.envelope = null;
    }

    public static void main(String[] argv) throws Exception
    {
        PropertyConfigurator.configure("log4j.properties");

        final int num_proc ;
        final int num_docs;
        final String log_token;
        if (argv.length==1)
        {
            num_proc=Integer.parseInt(argv[0]);
            num_docs=1;
            log_token="test";
            instance.db_name="test";

        }
        else if(argv.length==2)
        {
            num_proc=Integer.parseInt(argv[0]);
            num_docs=Integer.parseInt(argv[1]);
            log_token="test";
            instance.db_name="test";
        }
        else if(argv.length==3)
        {
            num_proc=Integer.parseInt(argv[0]);
            num_docs=Integer.parseInt(argv[1]);
            log_token=argv[2];
            instance.db_name="test";

        }
        else if(argv.length==4)
        {
            num_proc=Integer.parseInt(argv[0]);
            num_docs=Integer.parseInt(argv[1]);
            log_token=argv[2];
            instance.db_name=argv[3];
        }
        else
        {
            num_proc=1;
            num_docs=1;
            log_token="test";
            instance.db_name="test";
        }
        
        /*Creating SQlite DB to store output*/
        try {
            Class.forName("org.sqlite.JDBC");
            instance.c = DriverManager.getConnection("jdbc:sqlite:"+instance.db_name+".db");
            PreparedStatement stmt = instance.c.prepareStatement("CREATE TABLE IF NOT EXISTS json_test_table (id VARCHAR , date VARCHAR, output VARCHAR, mongo_id VARCHAR)");
            stmt.executeUpdate();
        } catch ( Exception e ) {
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
            instance.log.debug("Error with SQlite");

        }
        instance.log.debug("Database successfully connected");
        
        /* Create a file or read to know the restart status of the file */

        File restart_file= new File("restart_status.txt");
        if(!restart_file.exists())
        {
            restart_file.createNewFile();
            FileWriter fw= new FileWriter(restart_file.getAbsoluteFile());
            BufferedWriter bw= new BufferedWriter(fw);
            bw.write(log_token);
            bw.write(":started");
            bw.close();
        }
        else
        {
            BufferedReader br= new BufferedReader(new FileReader(restart_file.getAbsoluteFile()));
            String []data= br.readLine().split(":");
            restart_status=data[1];

        }
        
        System.out.println("Batch size: "+num_docs);
        System.out.println("#threads: "+num_proc);
        instance.log.debug(log_token+" #Threads: "+num_proc+" #Batch_size: "+num_docs);

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

        Timer timer= new Timer();
        
        // Flush documents code
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
                        doWork(num_proc, envArrayList.size(), log_token, true);
                        instance.channel.basicAck(envArrayList.get(last_index-1).getDeliveryTag(), true);
                        last_index=0;
                        envArrayList.clear();
                        System.exit(1);
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
        //**************************************************************//

        //*Batch timer thread
        Timer batch_timer_thread= new Timer();
        batch_timer_thread.schedule(new TimerTask() {
        	String last_doc =instance.last_processed_doc_id;
        	@Override
            public void run() {
                if(instance.batch_timer.isRunning())
                {
                	System.out.println("last doc_id: "+last_doc);
                    if( instance.last_processed_doc_id.equals(last_doc))
                    {
                    	try {
							instance.channel.basicAck(instance.envelope.getDeliveryTag(), true);
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
                        instance.log.error(log_token+"Taking too long for batch to execute...Restarting the container ");
                        System.exit(1);
                    }
                    else
                    {
                    	last_doc= instance.last_processed_doc_id;
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
            instance.channel.basicQos(2*num_docs);

            DefaultConsumer consumer_rabbimq = new DefaultConsumer(instance.channel)
            {
                 
                int ackCount=0;

                @Override
                public void handleDelivery(String consumerTag, Envelope envelope,
                                           AMQP.BasicProperties properties, byte[] body) throws IOException
                {
                    instance.envelope = envelope; 
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
                        instance.batch_data_bytes+=article_body.getBytes("UTF-8").length;
                         
                        Annotation annotation = new Annotation(article_body);
                        annotation.set(CoreAnnotations.DocIDAnnotation.class, doc_id);
                        annotation.set(CoreAnnotations.DocDateAnnotation.class, pub_date);
                        annotation.set(CoreAnnotations.DocTitleAnnotation.class,mongo_id);
                        if(instance.restart_doc_count>=num_docs)
                            restart_status="empty";
                        if(restart_status.equals("started") && instance.restart_doc_count<=num_docs)
                        {
                            if(instance.mongoArrayList.size()<=0)
                            {
                                instance.log.debug(log_token+" Container restarted and fetching documents");
                                instance.mongoArrayList=new sqlite_reader().doc_present(instance.db_name,num_docs);
                            }
                            if(!instance.mongoArrayList.contains(mongo_id))
                            {
                            	ackCount++;
                                instance.queue.put(annotation);
                                instance.env_queue.put(envelope);
                                instance.flush_timer.reset();
                                instance.flush_timer.start();

                            }
                        }
                        else
                        {
                            ackCount++;
                            instance.queue.put(annotation);
                            instance.env_queue.put(envelope);
                            instance.flush_timer.reset();
                            instance.flush_timer.start();

                        }
                        doWork(num_proc,num_docs,log_token,false);
                    }
                    //exit error may not be a parser exception
                    catch (Exception e)
                    {
                        // TODO Auto-generated catch block
                        try
                        {
                            File file = new File(log_token+"_"+".exitlog");

                            
                            if (!file.exists()) {
                                file.createNewFile();
                            }
                             
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
                    {  
                        if(ackCount%num_docs==0)
                        {
                            
                        	try
                        	{
                        		instance.channel.basicAck(envelope.getDeliveryTag(), true);
                        	}
                        	catch (com.rabbitmq.client.AlreadyClosedException e)
                        	{
                        		instance.log.error("Rabbit Mq Connection Closed");
                        		System.exit(1);
                        	}
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
    }
    private static void doWork(int num_proc,int num_docs,final String log_token,boolean flush) throws Exception {
         

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
                synchronized (instance)
                {
                    instance.pipeline.annotate(tempArraylist,num_proc , new Consumer<Annotation>() {

                        public void accept(Annotation arg0)
                        {
                            instance.docs_parsed++;
                            String doc_id= arg0.get(CoreAnnotations.DocIDAnnotation.class);
                            String pub_date=arg0.get(CoreAnnotations.DocDateAnnotation.class);
                            String mongo_id=arg0.get(CoreAnnotations.DocTitleAnnotation.class);
                           
                            JSONObject doc_out= new JSONObject();; // main object
                            doc_out.put("doc_id", doc_id);
                            JSONArray sen_array= new JSONArray();
                            instance.current_processing_doc_id = mongo_id;
                            System.out.println("Processing"+instance.current_processing_doc_id);
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
                                    instance.last_processed_doc_id = mongo_id;
                                    System.out.println("Processed: "+mongo_id);
                                	instance.log.debug(log_token+": "+ ++instance.docs_inserted+": Successfully inserted");
                                    instance.io_operation++;
                                }
                                else
                                {
                                    instance.log.error(log_token+"ERROR in inserting document");
                                }

                            } 
                            catch (SQLException e) {
                                e.printStackTrace();
                                instance.log.error(log_token+"Exception in inserting documents");
                            }
                        }
                    });
                }
            }
             
            instance.stats.insert_data("stats", String.valueOf(num_proc), String.valueOf(num_docs), String.valueOf(instance.batch_data_bytes), String.valueOf(instance.sents_parsed), String.valueOf(instance.io_operation),String.valueOf(instance.batch_timer.elapsed(TimeUnit.SECONDS)));
            instance.io_operation=0;
            instance.batch_data_bytes=0;
            instance.sents_parsed=0;
            instance.previous_time=(int) instance.batch_timer.elapsed(TimeUnit.SECONDS);
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
}
