import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.google.common.base.Stopwatch;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;

import edu.stanford.nlp.coref.CorefCoreAnnotations;
import edu.stanford.nlp.coref.data.CorefChain;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreAnnotations.LemmaAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.semgraph.SemanticGraph;
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.trees.TreeCoreAnnotations.TreeAnnotation;
import edu.stanford.nlp.util.CoreMap;
public class corenlp_worker 
{
	//Stanford Corenlp Variables
    private final static corenlp_worker instance;
    private Properties props = new Properties();
    private StanfordCoreNLP corenlp_pipeline;
    private ArrayList < Annotation > annotation_documents_list = new ArrayList < Annotation > ();
    
    //Pipeline Variables 
    private int batch_size;
    private int threads;
    private String db_name;
    private Integer cores;
    private int total_docs_processed;
    private int batch_docs_processed;
    private int total_sentences_processed;
    private String restart_status = "normal";
    private boolean is_pipeline_active = false;
    private String previous_processed_doc = "previous";
    private String current_processed_doc = "current";
    
    //logging variables
    private Logger log = LogManager.getLogger("corenlp_worker");
    private String log_token;
    
    //rabbitmq variables
    private String TASK_QUEUE_NAME;
    private Channel channel;
    private Envelope envelope;
    private DefaultConsumer consumer;
    
    //Sqlitedb and checking for duplicate records if container restarts varaibales
    
    private java.sql.Connection c;
    private stats stats;
    private ArrayList < String > mongo_array_list = new ArrayList < String > ();
    
    //timing information for total pipeline
    private Stopwatch batch_timer = Stopwatch.createUnstarted();
    private Stopwatch total_timer = Stopwatch.createStarted();
    private int previous_batch_time = 0;
    private int batch_data_bytes = 0;
    private int io_operation = 0;
    
    //timing information for batch
    private Stopwatch rabbitmq_time = Stopwatch.createUnstarted();
    private Stopwatch startup_time = Stopwatch.createStarted();
    private int tokens_per_batch = 0;
    private int sentences_per_batch=0;
    private int lemmas_per_batch=0;
    private int ners_per_batch=0;
    private int parses_per_batch=0;
    private int dcorefs_per_batch=0;
    private int sentiments_per_batch=0;
    private int dependencies_per_batch =0 ;
    private int total_batch_size = 0;
    private long tokenize_time =0;
    private long ssplit_time = 0;
    private long dependency_time =0;
    private long lemma_time = 0;
    private long ner_time = 0;
    private long parse_time =0;
    private long dcoref_time =0;
    private long sentiment_time =0;
    private long insertion_time =0;
    private long json_object_time = 0;
    
    //Timer for checking different cases
    private Timer timer = new Timer();
    
    //Adaptive batch
    private boolean prediction = false;
    private long predicted_time = 0;
    private boolean batch_size_inc = true;
    private boolean thread_size_inc = false;
    private int batch_variator = 10;
    private int thread_variator = 4;
    private String consumer_tag= UUID.randomUUID().toString();
    private String pipeline_id= UUID.randomUUID().toString();
    
    /*
    // Kalman Filter
    private double Z = 0;  // Observations in this case our docs size in a batch
    private double Q = 1*Math.pow(10,-5);   
    private double xhat = 0.0; // a posteri estimate of x
    private double P = 1; //  posteri error estimate 
    private double K = 0.0; // gain or blending factor
    private double R = Math.pow(0.1, 5); // Change R to see the effect
    private int kalam_docs_size = Integer.MAX_VALUE;
    private int max_batch_size = Integer.MAX_VALUE;
    */
	
	private ArrayList<Integer> docs_sizes_batch = new ArrayList<>();
   
    
    static 
    {
        instance = new corenlp_worker();
	instance.props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner, parse, dcoref,sentiment");
        instance.cores = Runtime.getRuntime().availableProcessors();
        //instance.props.setProperty("threads", instance.cores.toString());
        instance.props.setProperty("parse.model", "edu/stanford/nlp/models/srparser/englishSR.ser.gz");
        instance.corenlp_pipeline = new StanfordCoreNLP(instance.props);
        instance.startup_time.stop();
        instance.total_docs_processed = 0;
        instance.batch_docs_processed = 0;
        instance.total_sentences_processed = 0;
        
    }

    public static void main(String[] argv) 
    {
    	
        if (argv.length == 1) 
        {
            instance.threads = Integer.parseInt(argv[0]);
            instance.batch_size = 1;
            instance.log_token = "test";
            instance.db_name = "test";
            instance.total_batch_size = instance.batch_size;
        }
        else if (argv.length == 2) 
        {
            instance.threads = Integer.parseInt(argv[0]);
            instance.batch_size = Integer.parseInt(argv[1]);
            instance.log_token = "test";
            instance.db_name = "test";
            instance.total_batch_size = instance.batch_size;
        } 
        else if (argv.length == 3) 
        {
            instance.threads = Integer.parseInt(argv[0]);
            instance.batch_size = Integer.parseInt(argv[1]);
            instance.log_token = argv[2];
            instance.db_name = "test";
            instance.total_batch_size = instance.batch_size;
        } 
        else if (argv.length == 4) 
        {
            instance.threads = Integer.parseInt(argv[0]);
            instance.batch_size = Integer.parseInt(argv[1]);
            instance.log_token = argv[2];
            instance.db_name = argv[3];
            instance.total_batch_size = instance.batch_size;
        }
        else if (argv.length == 5) 
        {
            instance.threads = Integer.parseInt(argv[0]);
            instance.batch_size = Integer.parseInt(argv[1]);
            instance.log_token = argv[2];
            instance.db_name = argv[3];
            instance.total_batch_size = Integer.parseInt(argv[4]);
        }
        else 
        {
            instance.threads = instance.cores;
            instance.batch_size = 1;
            instance.log_token = "test";
            instance.db_name = "test";
            instance.total_batch_size = instance.batch_size;
        }
   
        instance.stats = new stats(instance.db_name);
        //instance.log.debug("PIPELINEERROR"+instance.log_token+"Container Started");
        
        //instance.max_batch_size = instance.batch_size;
        
        instance.timer.schedule(new TimerTask() 
       	{
            @Override
            public void run() 
            {
                System.out.println(instance.previous_processed_doc);
                System.out.println(instance.current_processed_doc);
                System.out.println((int) instance.batch_timer.elapsed(TimeUnit.SECONDS));
                System.out.println("Pipeline status:" + instance.is_pipeline_active);
                
                // CASE 1:
                //
                //
                
                /*if (instance.previous_batch_time > 0 && instance.is_pipeline_active) 
                {
                    System.out.println(instance.previous_batch_time);
                    if ((int) instance.batch_timer.elapsed(TimeUnit.SECONDS) > 2 * instance.previous_batch_time) 
                    {
                        instance.log.debug(instance.log_token + " " + "Taking So long to Process! Restating the container");
                        System.exit(1);
                    }
                }*/
                
                // CASE 2: Consider the situation where total documents to process are 900 and batch size is 500. According to logic in the program we are starting the
                // Stanford corenlp pipeline once we reach batch size documents i.e 500. For the second run we have only 400 documents (900-500). So if we check if pipeline status
                // is false and are there any documents present to be processed.
                
                if (!instance.is_pipeline_active && instance.annotation_documents_list.size() > 0) 
                {
                   
                    try 
                    {
                    	System.out.println("Checking for "+instance.threads+" Threds and "+instance.annotation_documents_list.size()+" BatchSize");
                    	System.out.println("Current doc Size "+instance.batch_data_bytes);
                    	
                    	batch_info bi = instance.stats.getAvgTime(instance.db_name);
                    	if (bi.avgTimeTaken > 0)
                    	{
                    		System.out.println("Avg Doc Size "+bi.avgDocSize);
                    		System.out.println("Avg Time" +bi.avgTimeTaken);
                    		
                    		instance.prediction = true;
                    		instance.predicted_time = (bi.avgTimeTaken * instance.batch_data_bytes) / bi.avgDocSize;
                    		System.out.println("Predicted Time "+instance.predicted_time);
                    	}
                    	else
                    	{
                    		System.out.println("No Previous Information Found");
                    		instance.prediction = false;
                    	}
                    	
                    	instance.log.debug(instance.log_token + " " + "Processing remaining Documents");
                    	doWork(instance.annotation_documents_list, instance.threads);                    	
                        
                         
                    } 
                    catch (Exception e) 
                    {
                        instance.log.error(getExceptionSting(e));
                    }
                }
                
                // CASE 3: 
                //
                //
                
                if (instance.previous_processed_doc.equals(instance.current_processed_doc)) 
                {
                    try 
                    {
                    	
                        instance.log.debug(instance.log_token + " " + "Pipeline is stuck! Restarting container");
                        instance.channel.basicAck(instance.envelope.getDeliveryTag(), true);
                        instance.log.debug("Stuck pipeline... Auto ACK Messages");
                    } 
                    catch (IOException e) 
                    {
                        instance.log.error(getExceptionSting(e));
                    }
                    System.exit(1);
                }
                else 
                {
                    instance.previous_processed_doc = instance.current_processed_doc;
                }

            }
        }, 0, 60000 * 1);

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
            instance.c = DriverManager.getConnection("jdbc:sqlite:" + instance.db_name + ".db");
            PreparedStatement stmt = instance.c.prepareStatement("CREATE TABLE IF NOT EXISTS json_test_table (id VARCHAR , date VARCHAR, output VARCHAR, mongo_id VARCHAR)");
            stmt.executeUpdate();

            /* Create a file or read to know the restart status of the file */
            File restart_file = new File("restart_status.txt");
            if (!restart_file.exists()) 
            {
                restart_file.createNewFile();
                FileWriter fw = new FileWriter(restart_file.getAbsoluteFile());
                BufferedWriter bw = new BufferedWriter(fw);
                bw.write(instance.log_token);
                bw.write(":restarted::batchSize:"+instance.batch_size+"::threads:"+instance.threads);
                bw.close();
            } 
            else 
            {
                BufferedReader br = new BufferedReader(new FileReader(restart_file.getAbsoluteFile()));
                String[] data = br.readLine().split("::");
                instance.restart_status = data[0].split(":")[1];
                instance.batch_size = Integer.parseInt(data[1].split(":")[1]);
                instance.threads = Integer.parseInt(data[2].split(":")[1]);
                //instance.xhat = Double.parseDouble(data[3].split(":")[1]);
                //instance.P = Double.parseDouble(data[4].split(":")[1]);
                //instance.K = Double.parseDouble(data[5].split(":")[1]);
                //instance.max_batch_size = instance.batch_size;

                System.out.println(instance.restart_status);
                System.out.println("Last batch Size:"+instance.batch_size);
                System.out.println("Last Thread Size:"+instance.threads);
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
            instance.channel.addShutdownListener(new ShutdownListener() 
            {

                public void shutdownCompleted(ShutdownSignalException arg0) 
                {
                    instance.log.error("Rabbitmq Connection Closed... Restarting the container");
                    System.exit(1);
                }
            });

            instance.channel.queueDeclare(instance.TASK_QUEUE_NAME, true, false, false, null);
            instance.channel.basicQos(0);

            instance.consumer = new DefaultConsumer(instance.channel) 
            {
            	int ack_count = 0;
      
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException 
                {
                    try 
                    {
                    	//System.out.println("Receiving Documents");
                    	 
                    	if(!instance.rabbitmq_time.isRunning())
                    		instance.rabbitmq_time.start();
                    	
                        instance.envelope = envelope;
                        String message = new String(body, "UTF-8");
                         

                        document document = new document(message);
                       

                        if (instance.annotation_documents_list.size() == instance.batch_size)
                            instance.restart_status = "normal";

                        if (instance.restart_status.equals("restarted") && instance.annotation_documents_list.size() < instance.batch_size) 
                        {
                            if (instance.mongo_array_list.size() <= 0)
                            {
                            	System.out.println("Container Restarted");
                                instance.log.debug(instance.log_token + " Container restarted and fetching documents");
                                instance.mongo_array_list = new sqlite_reader().doc_present(instance.db_name, instance.batch_size);
                            }
                            if (!instance.mongo_array_list.contains(document.getMongo_id())) 
                            {
                            	if(document.getArticle_body()!=null)
                            	{
                            		if(document.getArticle_body().getBytes("UTF-8")!=null)
                            		{
                            			instance.annotation_documents_list.add(getAnnotation(document));
										
                            			int doc_size = document.getArticle_body().getBytes("UTF-8").length;
                            			instance.batch_data_bytes += doc_size;
                            			
                            			instance.docs_sizes_batch.add(doc_size);
                            			//System.out.println(document.getMongo_id());
                            			//System.out.println(instance.batch_data_bytes);
                            			ack_count++;
                            		}
                            	}
                            }
                        } 
                        else 
                        {
                        	if(document.getArticle_body()!=null)
                        	{
                        		if(document.getArticle_body().getBytes("UTF-8")!=null)
                        		{
                        			instance.annotation_documents_list.add(getAnnotation(document));
									
                        			int doc_size = document.getArticle_body().getBytes("UTF-8").length;
                            		instance.batch_data_bytes += doc_size;
                            			
                            		instance.docs_sizes_batch.add(doc_size);
                        			//System.out.println(document.getMongo_id());
                        			//System.out.println(instance.batch_data_bytes);
                        			
                        			ack_count++;
                        		}
                        	}
                        }
                        
                        //if (instance.annotation_documents_list.size() == instance.batch_size)
                        if (instance.annotation_documents_list.size() == instance.batch_size)
                        { 
				/*
                        	instance.batch_size = instance.annotation_documents_list.size();
                        	File restart_file = new File("restart_status.txt");
                            restart_file.createNewFile();
                            FileWriter fw = new FileWriter(restart_file.getAbsoluteFile());
                            BufferedWriter bw = new BufferedWriter(fw);
                            bw.write(instance.log_token);
                            bw.write(":restarted::batchSize:"+instance.batch_size+"::threads:"+instance.threads);
                            bw.close();
				*/
                            
			    /*
                            //code for dynamic batch and threads
                            
                        	System.out.println("Normal Pipeline");
                        	System.out.println("Checking for "+instance.threads+" Threds and "+instance.batch_size+" BatchSize");
                        	System.out.println("Current doc Size "+instance.batch_data_bytes);
                        	batch_info bi = instance.stats.getAvgTime(instance.db_name);
                        	if (bi.avgTimeTaken > 0)
                        	{
                        		System.out.println("Avg Doc Size "+bi.avgDocSize);
                        		System.out.println("Avg Time" +bi.avgTimeTaken);
                        		
                        		instance.prediction = true;
                        		instance.predicted_time = (bi.avgTimeTaken * instance.batch_data_bytes) / bi.avgDocSize;
                        		System.out.println("Predicted Time "+instance.predicted_time);
                        	}
                        	else
                        	{
                        		System.out.println("No Previous Information Found");
                        		instance.prediction = false;
                        	}
                        	
			      */
                        	instance.channel.basicCancel(instance.consumer_tag);
	
                        	doWork(instance.annotation_documents_list, instance.threads);
                        }
                        
                    }//end of try block
                    
                    catch (Exception e) 
                    {
                        instance.log.debug(getExceptionSting(e));
                        System.exit(1);
                    } 
                    finally 
                    {
                        //write code for acknowledge of rabbitmq
                    }
                }// end of handle delivery method 
            };// end of default customer method

            boolean autoAck = false;
            instance.channel.basicConsume(instance.TASK_QUEUE_NAME, autoAck, instance.consumer_tag,instance.consumer);


        } 
        catch (TimeoutException TOE) 
        {
            instance.log.error(getExceptionSting(TOE));
            TOE.printStackTrace();
            System.exit(1);
        } 
        catch (IOException IO) 
        {
            instance.log.error(getExceptionSting(IO));
            IO.printStackTrace();
            System.exit(1);
        } 
        catch (ClassNotFoundException CNE) 
        {
            instance.log.error(instance.log_token + ":" + getExceptionSting(CNE));
            CNE.printStackTrace();
            System.exit(1);
        } 
        catch (SQLException SQLE) 
        {
            instance.log.error(instance.log_token + ":" + getExceptionSting(SQLE));
            SQLE.printStackTrace();
            System.exit(1);
        } 
        catch (ParseException PE) 
        {
            instance.log.error(instance.log_token + ":" + getExceptionSting(PE));
            PE.printStackTrace();
            System.exit(1);
        } 
        catch (Exception e) 
        {
            instance.log.error(instance.log_token + ":" + getExceptionSting(e));
            e.printStackTrace();
            System.exit(1);
        }
    }// end of main method

    private static void doWork(ArrayList < Annotation > annotaion_documents_list, int num_threads) 
    {
	if(instance.rabbitmq_time.isRunning())	
    		instance.rabbitmq_time.stop();
		
	int min_doc_size = Collections.min(instance.docs_sizes_batch);
	int max_doc_size = Collections.max(instance.docs_sizes_batch);
	double mean_doc_size = instance.docs_sizes_batch.stream().mapToInt(val -> val).average().getAsDouble();
	
	instance.docs_sizes_batch.clear();

	instance.batch_size = annotaion_documents_list.size();
	try
	{ 
        	File restart_file = new File("restart_status.txt");
		restart_file.createNewFile();
       		FileWriter fw = new FileWriter(restart_file.getAbsoluteFile());
        	BufferedWriter bw = new BufferedWriter(fw);
        	bw.write(instance.log_token);
		bw.write(":restarted::batchSize:"+instance.batch_size+"::threads:"+instance.threads);        
        	bw.close();
	}
	catch (Exception e)
        {
            instance.log.error(instance.log_token + ":" + getExceptionSting(e));
            e.printStackTrace();
        }   
    	
    	int num_docs = annotaion_documents_list.size();
        System.out.println(instance.batch_size + " " + num_threads);
        
        instance.log.debug(instance.log_token + " Started pipeline with #Threads:" + num_threads + " #Batch_size:" + num_docs);
        instance.is_pipeline_active = true;
        instance.batch_timer.start();
        instance.corenlp_pipeline.annotate(annotaion_documents_list, num_threads, new Consumer < Annotation > () 
        {
        	
            public void accept(Annotation anno) 
            {
            	Stopwatch tokenize_timer = Stopwatch.createUnstarted();
            	Stopwatch ssplit_timer = Stopwatch.createUnstarted();
            	Stopwatch dependency_timer = Stopwatch.createUnstarted();
            	Stopwatch lemma_timer = Stopwatch.createUnstarted();
            	Stopwatch ner_timer = Stopwatch.createUnstarted();
            	Stopwatch parse_timer = Stopwatch.createUnstarted();
            	Stopwatch dcoref_timer = Stopwatch.createUnstarted();
            	Stopwatch sentiment_timer = Stopwatch.createUnstarted();
            	Stopwatch insertion_timer = Stopwatch.createUnstarted();
            	Stopwatch json_object_timer = Stopwatch.createUnstarted();
            	
                String doc_id = anno.get(CoreAnnotations.DocIDAnnotation.class);
                String pub_date = anno.get(CoreAnnotations.DocDateAnnotation.class);
                String mongo_id = anno.get(CoreAnnotations.DocTitleAnnotation.class);

                instance.log.debug(doc_id + ": PARSING");
                instance.current_processed_doc = mongo_id;
                instance.log.debug(instance.log_token + " Processed:" + mongo_id);
                
                json_object_timer.start();
                JSONObject doc_out = new JSONObject();; // main object
                doc_out.put("doc_id", doc_id);
                JSONArray sen_array = new JSONArray();
                json_object_timer.stop();

           
                ssplit_timer.start();
                List <CoreMap> sentences = new ArrayList<CoreMap>();
                sentences = anno.get(SentencesAnnotation.class);
                ssplit_timer.stop();
                
                instance.sentences_per_batch = instance.sentences_per_batch + sentences.size();
                instance.total_sentences_processed = instance.total_sentences_processed + instance.sentences_per_batch;
                
                Integer sen_id = 0;
                 
                ArrayList < String > tokens = new ArrayList < String > ();
                ArrayList < String > lemmas = new ArrayList < String > ();
                ArrayList < String > ners = new ArrayList < String > ();

                for (CoreMap sentence: sentences) 
                {
                    for (CoreLabel token: sentence.get(TokensAnnotation.class)) 
                    {

                        // this is the text of the token
                    	tokenize_timer.start();
                        String word = token.get(TextAnnotation.class);
                        tokens.add(word);
                        tokenize_timer.stop();

                        //this is the text of the the lemma
                        lemma_timer.start();
                        String lemma = token.get(LemmaAnnotation.class);
                        lemmas.add(lemma);
                        lemma_timer.stop();

                        // this is the POS tag of the token
                        //String pos = token.get(PartOfSpeechAnnotation.class);

                        // this is the NER label of the token
                        ner_timer.start();
                        String ner = token.get(NamedEntityTagAnnotation.class);
                        ners.add(ner);
                        ner_timer.stop();

                    }

                    instance.tokens_per_batch = instance.tokens_per_batch + tokens.size();
                    instance.lemmas_per_batch = instance.lemmas_per_batch + lemmas.size();
                    instance.ners_per_batch = instance.ners_per_batch + ners.size();
                    
                    parse_timer.start();
                    Tree tree = sentence.get(TreeAnnotation.class);
                    parse_timer.stop();
                    instance.parses_per_batch++;
                     
                    sentiment_timer.start();
                    String sentiment = sentence.get(SentimentCoreAnnotations.SentimentClass.class);
                    sentiment_timer.stop();
                    instance.sentiments_per_batch++;
                    
                    dependency_timer.start();
                    String dependencies = sentence.get(SemanticGraphCoreAnnotations.BasicDependenciesAnnotation.class).toString(SemanticGraph.OutputFormat.LIST);
                    dependency_timer.stop();
                    instance.dependencies_per_batch++;

                    //System.out.println(sentiment);
                    json_object_timer.start();
                    JSONObject sen_obj = new JSONObject(); // sentence object;
                    sen_obj.put("sentence_id", (++sen_id).toString());
                    sen_obj.put("sentence", sentence.toString());
                    sen_obj.put("parse_sentence", tree.toString());
                    sen_obj.put("dependency_tree", dependencies.toString());
                    sen_obj.put("token", tokens.toString());
                    sen_obj.put("lemma", lemmas.toString());
                    sen_obj.put("ner", ners.toString());
                    sen_obj.put("sentiment", sentiment.toString());
                    sen_array.add(sen_obj);
                    json_object_timer.stop();
                }
                
                dcoref_timer.start();
                ArrayList < String > corefs = new ArrayList < String > ();
                Map < Integer, CorefChain > corefChains = anno.get(CorefCoreAnnotations.CorefChainAnnotation.class);
                for (Map.Entry < Integer, CorefChain > entry: corefChains.entrySet()) 
                {
                    corefs.add(entry.getValue().toString());
                }

                doc_out.put("sentences", sen_array);
                if (corefChains != null)
                {
                    doc_out.put("coref", corefs.toString());
                    instance.dcorefs_per_batch = instance.dcorefs_per_batch + corefs.size();
                }
                else
                {
                    doc_out.put("coref", "");
                    instance.dcorefs_per_batch = instance.dcorefs_per_batch;
                }
                dcoref_timer.stop();
                instance.log.debug(doc_id + ": PARSED");
                 
                
                insertion_timer.start();
                try 
                {
                    PreparedStatement stmt = instance.c.prepareStatement("INSERT INTO json_test_table (id,date,output,mongo_id) VALUES (?,?,?,?)");
                    stmt.setString(1, doc_id.toString());
                    stmt.setString(2, pub_date.toString());
                    stmt.setString(3, doc_out.toJSONString());
                    stmt.setString(4, mongo_id);

                    if (stmt.executeUpdate() == 1) 
                    {
                        instance.io_operation++;
                        instance.total_docs_processed++;
                        instance.batch_docs_processed++;
                        instance.log.debug(instance.log_token + ":" + instance.total_docs_processed + " Inserted"+"Batch Size "+instance.batch_size);
                    } 
                    else 
                    {
                        instance.log.error(instance.log_token + "ERROR in inserting document");
                    }

                } 
                catch (SQLException e) 
                {
                    e.printStackTrace();
                    instance.log.error(instance.log_token + "Exception in inserting documents");
                }
                insertion_timer.stop();
                
                
                //storing all timing information
                instance.tokenize_time = instance.tokenize_time+tokenize_timer.elapsed(TimeUnit.NANOSECONDS);
                instance.ssplit_time = instance.ssplit_time+ ssplit_timer.elapsed(TimeUnit.NANOSECONDS);
                instance.dependency_time = instance.dependency_time+ dependency_timer.elapsed(TimeUnit.NANOSECONDS);
                instance.lemma_time = instance.lemma_time+ lemma_timer.elapsed(TimeUnit.NANOSECONDS);
                instance.ner_time = instance.ner_time+ ner_timer.elapsed(TimeUnit.NANOSECONDS);
                instance.parse_time = instance.parse_time+ parse_timer.elapsed(TimeUnit.NANOSECONDS);
                instance.dcoref_time = instance.dcoref_time+ dcoref_timer.elapsed(TimeUnit.NANOSECONDS);
                instance.sentiment_time = instance.sentiment_time+ sentiment_timer.elapsed(TimeUnit.NANOSECONDS);
                instance.insertion_time = instance.insertion_time+ insertion_timer.elapsed(TimeUnit.NANOSECONDS);
                instance.json_object_time = instance.json_object_time+ json_object_timer.elapsed(TimeUnit.NANOSECONDS);
                
            }// end of accept annotation
         
        }); // end of annotate pipeline method
        
        //System.out.println(instance.corenlp_pipeline.timingInformation());
        
       /* Long actual_batch_time = instance.batch_timer.elapsed(TimeUnit.NANOSECONDS);
        System.out.println("Predicted Time "+instance.predicted_time);
        System.out.println("Actual Time "+actual_batch_time);
        
        HashMap<String, Double> Kalman_values = kalman_filter(instance.xhat, instance.P, instance.Q, instance.R, instance.K, actual_batch_time);
        
        instance.xhat = Kalman_values.get("xhat");
        instance.P = Kalman_values.get("P");
        instance.K = Kalman_values.get("K");
        
        String kalman_time = BigDecimal.valueOf(instance.xhat).toPlainString();
        
        instance.kalam_docs_size = kalman_datasize(instance.xhat, actual_batch_time, instance.batch_data_bytes);
        System.out.println("Kalman Values:"+ kalman_time);
        
        System.out.println("Prev docs size "+instance.batch_data_bytes);
        System.out.println("new docs Size "+instance.kalam_docs_size);
        
        */
        
        
        /*
        try
        {
        
        	if(instance.prediction && instance.predicted_time > 0)
            {
            	if(actual_batch_time <= instance.predicted_time)
                {
                	System.out.println("Increase ");
                	if(instance.batch_size_inc)
                		instance.batch_size+= instance.batch_variator;
                	if(instance.thread_size_inc)
                		instance.threads+=instance.thread_variator;
                	 
                }
                else
                {
                	System.out.println("Decrease");
                	if(instance.batch_size_inc)
                	{
                		instance.batch_size-= instance.batch_variator;
                		if(instance.batch_size < instance.batch_variator)
                			instance.batch_size+=instance.batch_variator;
                	}
                	if(instance.thread_size_inc)
                	{
                		instance.threads-=instance.thread_variator;
                		if(instance.threads<instance.cores)
                			instance.threads = instance.cores;
                	}
                }
            }
        }
        
        catch (Exception e) 
        {
			e.printStackTrace();
		}
        */
        
        instance.log.debug(instance.log_token +"Pipeline_Timining "+instance.corenlp_pipeline.timingInformation().toString().trim().replaceAll("\n", ""));
          
        	instance.stats.insert_data
        	(
        			instance.pipeline_id,
        			instance.db_name,
        			String.valueOf( num_threads),
        			String.valueOf(num_docs),
        			String.valueOf(instance.total_docs_processed),
        			
        			String.valueOf(instance.batch_data_bytes),
        			String.valueOf(instance.sentences_per_batch),
        			String.valueOf(instance.tokens_per_batch),
        			String.valueOf(instance.lemmas_per_batch),
        			String.valueOf(instance.ners_per_batch),
        			String.valueOf(instance.parses_per_batch),
        			String.valueOf(instance.dcorefs_per_batch),
        			String.valueOf(instance.sentiments_per_batch),
        			String.valueOf(instance.dependencies_per_batch), 
        			String.valueOf(instance.io_operation),

        			String.valueOf(instance.batch_timer.elapsed(TimeUnit.NANOSECONDS)),
        			String.valueOf(0),
        			String.valueOf(instance.total_timer.elapsed(TimeUnit.NANOSECONDS)),
        			String.valueOf(instance.rabbitmq_time.elapsed(TimeUnit.NANOSECONDS)),        			
        			String.valueOf(instance.tokenize_time),
        			String.valueOf(instance.ssplit_time),
        			String.valueOf(instance.dependency_time),
        			String.valueOf(instance.lemma_time),
        			String.valueOf(instance.ner_time),
        			String.valueOf(instance.parse_time),
        			String.valueOf(instance.dcoref_time),
        			String.valueOf(instance.sentiment_time),
        			String.valueOf(instance.insertion_time),
        			String.valueOf(instance.json_object_time),
        			String.valueOf(instance.startup_time.elapsed(TimeUnit.NANOSECONDS)),
					String.valueOf(min_doc_size),
        			String.valueOf(max_doc_size),
        			String.valueOf(mean_doc_size)
        			
        	);
        	
        	instance.batch_data_bytes = 0;
        	instance.sentences_per_batch=0;
            instance.tokens_per_batch = 0;
            instance.lemmas_per_batch=0;
            instance.ners_per_batch=0;
            instance.parses_per_batch=0;
            instance.dcorefs_per_batch=0;
            instance.sentiments_per_batch=0;
            instance.dependencies_per_batch =0 ;
            instance.io_operation = 0;
             
            instance.tokenize_time = 0;
			instance.ssplit_time = 0;
			instance.dependency_time = 0;
			instance.lemma_time = 0;
			instance.ner_time = 0;
			instance.parse_time = 0;
			instance.dcoref_time = 0;
			instance.sentiment_time = 0; 
			instance.insertion_time = 0;
			instance.json_object_time = 0;
			instance.rabbitmq_time.reset();
			
			//instance.max_batch_size = Integer.MAX_VALUE;
           
        instance.annotation_documents_list.clear();
        instance.is_pipeline_active = false;
        instance.previous_batch_time = (int) instance.batch_timer.elapsed(TimeUnit.SECONDS);
        instance.log.debug(instance.log_token + "#Documents:" + instance.total_docs_processed +"#Sentences:"+instance.total_sentences_processed +":Processed::Time:" + instance.total_timer);
        instance.batch_timer.stop();
        instance.batch_timer.reset();
        System.out.println("Batch Timer Stopped");
        

        
        if (instance.batch_docs_processed % num_docs != 0) 
        {
            instance.log.debug(instance.log_token + ":Document is stuck... Restart container");
            System.exit(1);
        }
        instance.batch_docs_processed = 0;
        
        System.out.println("New Batch Size: "+instance.batch_size);
    	System.out.println("New Thread Size: "+instance.threads);
    	
    	try 
    	{
    		//instance.channel.basicConsume(instance.TASK_QUEUE_NAME, false, instance.consumer_tag,instance.consumer);
    		System.out.println("Sending Ack");
			instance.channel.basicAck(instance.envelope.getDeliveryTag(), true);
			
			//instance.channel.basicCancel(instance.consumer_tag);
			instance.consumer_tag= UUID.randomUUID().toString();
        	instance.channel.basicQos(0);
        	instance.channel.basicConsume(instance.TASK_QUEUE_NAME, false, instance.consumer_tag,instance.consumer);
        	System.out.println("New consumer Created");
		} 
    	catch (Exception e) 
    	{
			// TODO Auto-generated catch block
    		e.printStackTrace();
			instance.log.error(instance.log_token + "Failed to send acknowledgement");
			System.exit(0);
		}
    	
        //System.exit(1);
	
    }// end of dowork method
    
    public static Annotation getAnnotation(document document)
    {
        Annotation annotation = new Annotation(document.getArticle_body());
        annotation.set(CoreAnnotations.DocIDAnnotation.class, document.getDoc_id());
        annotation.set(CoreAnnotations.DocDateAnnotation.class, document.getPubclication_date());
        annotation.set(CoreAnnotations.DocTitleAnnotation.class, document.getMongo_id());
        return annotation;
    }
    
    public static String getExceptionSting(Exception e) 
    {
        StringWriter errors = new StringWriter();
        e.printStackTrace(new PrintWriter(errors));
        return errors.toString();
    }
    
    public static HashMap<String, Double> kalman_filter(double xhat, double P, double Q, double R, double K, double Z)
    {
    	// Time Update
    	double xhat_minus = xhat;
    	double P_minus = P + Q;
    	
    	 
    	K = P_minus / (P_minus + R);
    	xhat = xhat_minus + K * (Z - xhat_minus);
    	P = (1 - K) * P_minus;
    	
    	HashMap<String, Double> kalman_values = new HashMap<>();
    	
    	kalman_values.put("xhat", xhat);
    	kalman_values.put("P", P);
    	kalman_values.put("K", K);
    	
     	
    	return kalman_values;
    	
    }
    
    public static int kalman_datasize(double xhat, double actual_time, int docssize)
    {
    	return (int)((docssize*xhat)/actual_time);
    }
}
