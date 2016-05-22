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
    private static String restart_status="empty";
    private int restart_doc_count;
    private ArrayList<String> mongoArrayList;
    private sqlite_reader util_db;
    private int batch_data_bytes;
    private int sents_parsed;
    private int previous_batch_time;
    private boolean primary_start;
    private float estimated_time;
    private String R_queue;
    private int batch_size;
    private String consumer_tag;
    private DefaultConsumer consumer_rabbimq;
    private int batch_variator;
    private int ackCount;
    private ArrayList<Long> mem_info;
    private ArrayList<Long> time_info; 

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
        instance.restart_doc_count=0;
        instance.mongoArrayList=new ArrayList<String>();
        instance.util_db=new sqlite_reader();
        instance.batch_data_bytes=0;
        instance.sents_parsed=0;
        instance.previous_batch_time=0;
        instance.primary_start=true;
        instance.estimated_time=0;
        instance.batch_size=0;
        instance.consumer_tag=UUID.randomUUID().toString();
        instance.batch_variator=500;
        instance.ackCount=0;
        instance.mem_info= new ArrayList<Long>();
        instance.time_info= new  ArrayList<Long>();
        try {
            Class.forName("org.sqlite.JDBC");
            instance.c = DriverManager.getConnection("jdbc:sqlite:test.db");
            PreparedStatement stmt = instance.c.prepareStatement("CREATE TABLE IF NOT EXISTS json_test_table (id VARCHAR , date VARCHAR, output VARCHAR, mongo_id VARCHAR)");
            stmt.executeUpdate();
            stmt=instance.c.prepareStatement("CREATE TABLE IF NOT EXISTS batch_info (batch_id VARCHAR , batch_size VARCHAR, data_size VARCHAR,sentences VARCHAR, batch_time VARCHAR)");
            stmt.executeUpdate();
        } catch ( Exception e ) {
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
            instance.log.debug("Error with SQlite");

        }
        instance.log.debug("Databases successfully Created");


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
            instance.batch_size=1;
            log_token="test";

        }
        else if(argv.length==2)
        {
            num_proc=Integer.parseInt(argv[0]);
            num_docs=Integer.parseInt(argv[1]);
            instance.batch_size=Integer.parseInt(argv[1]);
            log_token="test";
        }
        else if(argv.length==3)
        {
            num_proc=Integer.parseInt(argv[0]);
            num_docs=Integer.parseInt(argv[1]);
            instance.batch_size=Integer.parseInt(argv[1]);
            log_token=argv[2];

        }
        else
        {
            num_proc=1;
            num_docs=1;
            log_token="test";
        }

        /* Create a file or read to know the restart status of the file */

        File restart_file= new File("restart_status.txt");
        if(!restart_file.exists())
        {
            //System.out.println("File doesnot exits");
            restart_file.createNewFile();
            FileWriter fw= new FileWriter(restart_file.getAbsoluteFile());
            BufferedWriter bw= new BufferedWriter(fw);
            bw.write(log_token);
            bw.write(":started");
            bw.close();
        }
        else
        {
            //System.out.println("File exists");
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
        /*monitorThread.start();*/

        Timer timer= new Timer();
        timer.schedule(new TimerTask() {
            int last_index=0;
            ArrayList<Envelope>envArrayList= new ArrayList<Envelope>();
            @Override
            public void run() {
                // TODO Auto-generated method stub
                //System.out.println("Timer method Queue Size "+instance.queue.size());
                instance.env_queue.drainTo(envArrayList);
                last_index= envArrayList.size();
                //System.out.println("last index "+last_index);
                //System.out.println("env array list size "+envArrayList.size());
                //System.out.println(instance.flush_timer.elapsed(TimeUnit.MILLISECONDS));
                if(!instance.batch_timer.isRunning() && last_index>0 && instance.flush_timer.elapsed(TimeUnit.MILLISECONDS)>=60000)
                {
                    try
                    {
                        //System.out.println("executing thred do ork");
                        doWork(num_proc, envArrayList.size(), log_token, true);
                        instance.channel.basicAck(envArrayList.get(last_index-1).getDeliveryTag(), true);
                        last_index=0;
                        envArrayList.clear();
                        System.exit(1);
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
        }, 0,60000);
        //**************************************************************//

        //*Batch timer thread
        Timer batch_timer_thread= new Timer();
        batch_timer_thread.schedule(new TimerTask() {
            @Override
            public void run() {
                if(instance.batch_timer.isRunning() && instance.previous_batch_time!=0)
                {
                    if(instance.batch_timer.elapsed(TimeUnit.SECONDS)>2*instance.previous_batch_time)
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
            instance.channel.queueDeclare(instance.R_queue, true, false, false, null);
            instance.channel.basicQos(3*instance.batch_size+instance.batch_variator);

            instance.consumer_rabbimq = new DefaultConsumer(instance.channel)
            {
                //create this varaible to do batch acknowledgement
                

                @Override
                public void handleDelivery(String consumerTag, Envelope envelope,
                                           AMQP.BasicProperties properties, byte[] body) throws IOException
                {
                    //ackCount=ackCount+1;
                    String message = new String(body, "UTF-8");
                    //System.out.println(" [x] Received  messages'");
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

                        /*if the container has resarted then look if the doc id is present in the sqlite database and if
						 doc id is not present then add to the queue. Do this for first batch size*/
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
                                //System.out.println("Getiing documents");
                                instance.log.debug(log_token+" Container restarted and fetching documents");
                                instance.mongoArrayList=instance.util_db.doc_present("test",num_docs);
                            }
                            if(!instance.mongoArrayList.contains(mongo_id))
                            {
                                //System.out.println("restart:Doc Added to queue");
                                instance.ackCount++;
                                instance.queue.put(annotation);
                                instance.env_queue.put(envelope);
                                instance.flush_timer.reset();
                                instance.flush_timer.start();

                            }
                        }
                        else
                        {

                            //System.out.println("Non restart: doc Added");
                            instance.ackCount++;
                            instance.queue.put(annotation);
                            instance.env_queue.put(envelope);
                            instance.flush_timer.reset();
                            instance.flush_timer.start();

                        }
                        doWork(num_proc,instance.batch_size,log_token,false);


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
                    	//System.out.println("Ack count"+instance.ackCount);
                        if(instance.ackCount==instance.batch_size)
                        {
                            //System.out.println("Sending ACK");
                            instance.channel.basicAck(envelope.getDeliveryTag(), true);
                            instance.log.debug(log_token+": Changing batch: "+instance.batch_size);
                            //System.out.println(log_token+": Changing batch: "+instance.batch_size);
                            instance.channel.basicQos(3*instance.batch_size+instance.batch_variator);
                            instance.queue=new ArrayBlockingQueue<Annotation>(instance.batch_size);
                            instance.env_queue= new ArrayBlockingQueue<Envelope>(instance.batch_size);
                            //System.out.println("Consumer_tag: "+consumerTag);
                            //instance.channel.basicCancel(consumerTag);
                            //System.out.println(consumerTag+": Cancelled");
                            //instance.log.debug(log_token+" Consumer_tag: "+consumerTag+": Cancelled");
                            //instance.consumer_tag=UUID.randomUUID().toString();
                            //instance.channel.basicConsume(instance.R_queue, false,consumerTag,instance.consumer_rabbimq);
                            //System.out.println(instance.consumer_tag+": started");
                            //instance.log.debug(log_token+" Consumer_tag: "+consumerTag+": Started");
                            instance.ackCount=0;
                        }
                    }
                }
            };
            instance.channel.basicConsume(instance.R_queue, false, instance.consumer_tag,instance.consumer_rabbimq);
        }catch(ConnectException e2)
        {
            e2.printStackTrace();
            instance.log.error("Cannot connect to server, network error!");
        }
        finally
        {

        }
    }
    private static void doWork(int num_proc,int num_docs,final String log_token,boolean flush) throws Exception {
        //System.out.println("Inside dowork queue size"+instance.queue.size());
    	
        if(instance.queue.remainingCapacity()==0 || flush==true)
        {
        	//System.out.println("Batch size:"+instance.batch_size);
        	if(!instance.primary_start)
        	{
        		//System.out.println("Calculating optimal batch");
        		//instance.estimated_time=calculate_estimate_time(instance.batch_data_bytes);
        		//System.out.println("For: "+instance.batch_data_bytes);
        		//System.out.println(instance.estimated_time);
        		instance.estimated_time=get_avg_info(instance.mem_info, instance.time_info,instance.batch_data_bytes);
        	}
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
                            //System.out.println(mongo_id);
                            JSONObject doc_out= new JSONObject(); // main object
                            doc_out.put("doc_id", doc_id);
                            JSONArray sen_array= new JSONArray();
                            instance.log.debug(doc_id+": PARSING");
                            List<CoreMap> sentences = arg0.get(SentencesAnnotation.class);
                            Integer sen_id=0;
                            instance.sents_parsed+=sentences.size();
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
                                sen_obj.put("sen_id", (++sen_id).toString());
                                sen_obj.put("sentence", sentence.toString());
                                sen_obj.put("tree", tree.toString());
                                sen_array.add(sen_obj);
                                //SemanticGraph dependencies = sentence.get(CollapsedCCProcessedDependenciesAnnotation.class);
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
                                    //System.out.println("Doc inserted");
                                    //instance.c.commit(); //database is in auto commit mode

                                }
                                else
                                {
                                    instance.log.error(log_token+"ERROR in inserting document");
                                }

                            } catch (SQLException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                                instance.log.error(log_token+"Exception in inserting documents");
                            }
                        }
                    });

                }
                //System.out.println(instance.pipeline.getProperties());



            }
            //instance.log.debug(log_token+" #Documents: "+instance.docs_parsed);
            //String Stanford_timing=instance.pipeline.timingInformation();
            //instance.log.debug("Stanford Timing: "+Stanford_timing);
            //System.out.println(instance.batch_timer.elapsed(TimeUnit.SECONDS));
            if(!instance.primary_start)
            {
            	if(instance.batch_timer.elapsed(TimeUnit.SECONDS)<=instance.estimated_time)
                {
            		instance.batch_size+=instance.batch_variator;
            		instance.ackCount=instance.batch_size;
                	//System.out.println("Increase the number of docs");
                }
                else
                {
                	if(instance.batch_size-instance.batch_variator>0)
                		instance.batch_size-=instance.batch_variator;
                	instance.ackCount=instance.batch_size;
                	//System.out.println("Decrease number of docs");
                }
            }
            
            instance.primary_start=false;
            //System.out.println("Batch Size(bytes): "+instance.batch_data_bytes+"\nTime Taken(secs): "+Math.round((double)instance.batch_timer.elapsed(TimeUnit.SECONDS)));
            //instance.util_db.insert_batch_info("test",id, num_docs, instance.batch_data_bytes, instance.sents_parsed, (int) instance.batch_timer.elapsed(TimeUnit.SECONDS));
            instance.mem_info.add((long) instance.batch_data_bytes);
            instance.time_info.add(instance.batch_timer.elapsed(TimeUnit.SECONDS));
            instance.log.debug(log_token+" #Batch_Doc:" +instance.docs_parsed +" Batch time: "+instance.batch_timer);
            instance.previous_batch_time= (int) instance.batch_timer.elapsed(TimeUnit.SECONDS);
            instance.batch_timer.reset();
            instance.batch_data_bytes=0;
            instance.sents_parsed=0;
            instance.log.debug(log_token+" #Documents:" +instance.docs_inserted+" Total time: "+instance.total_timer);
            instance.flush_timer.reset();
            instance.flush_timer.start();
            tempArraylist.clear();
            instance.docs_parsed=0;
            instance.log.debug(log_token+": "+id+" Stanford Thread completed");


        }

    }
    public static float calculate_estimate_time(int batch_size)
    {
    	float avg_time=0;
    	float avg_size=0;
    	float estimate_time=0;
    	HashMap<String, Float> data=new HashMap<String, Float>();
    	data=instance.util_db.get_avg_info("test");
    	avg_time=data.get("avg_time");
    	avg_size=data.get("avg_size");
    	//System.out.println("Current batch size: "+batch_size);
    	//System.out.println("avg size: "+avg_size+"\navg_time: "+avg_time);
    	estimate_time=(avg_time*batch_size)/avg_size;
    	return estimate_time;
    }
    public static float get_avg_info(ArrayList<Long> mem, ArrayList<Long> time,int batch_size)
    {
    	float estimate_time=0;
    	float avg_mem=0;
    	float avg_time=0;
    	int size=mem.size();
    	for(int i=0;i<size;i++)
    	{
    		avg_mem+=mem.get(i);
    		avg_time+=time.get(i);
    	}
    	estimate_time=(avg_time*batch_size)/avg_mem;
    	return estimate_time;
    }
}


