import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class stats 
{
	Connection c;
	stats(String db_name)
	{
		 
		try {
            Class.forName("org.sqlite.JDBC");
            c = DriverManager.getConnection("jdbc:sqlite:"+db_name+"_stats.db");
            PreparedStatement stmt = c.prepareStatement("CREATE TABLE IF NOT EXISTS stats_table "
            		
            		+ "( "
            		+ "pipeline_id VARCHAR,"
            		+ "threads VARCHAR ,"
            		+ "batchSize VARCHAR,"
            		+ "totalbatchSize VARCHAR,"
            		+ " docsSize VARCHAR,"
            		+ "sentences VARCHAR,"
            		+ "tokens VARCHAR,"
            		+ "lemmas VARCHAR,"
            		+ "ners VARCHAR,"
            		+ "parses VARCHAR,"
            		+ "dcorefs VARCHAR,"
            		+ "sentiments VARCHAR,"
            		+ "dependencies VARCHAR,"
            		+ "io_operations VARCHAR,"
            		+ "batchTimeTaken VARCHAR,"
            		+ "kalmanTime VARCHAR,"
            		+ "timeTaken VARCHAR,"
            		+ "rabbitmq_time VARCHAR,"
            		+ "tokenize_time VARCHAR,"
            		+ "ssplit_time VARCHAR,"
            		+ "dependency_time VARCHAR,"
            		+ "lemma_time VARCHAR,"
            		+ "ner_time VARCHAR,"
            		+ "parse_time VARCHAR,"
            		+ "dcoref_time VARCHAR,"
            		+ "sentiment_time VARCHAR,"
            		+ "insertion_time VARCHAR,"
            		+ "json_object_time VARCHAR,"
            		+"startup_time VARCHAR,"
            		+"min_doc_size VARCHAR,"
            		+"max_doc_size VARCHAR,"
            		+"mean_doc_size VARCHAR"
            		+ ")"
            		
            		);
            
            stmt.executeUpdate();
             
        } catch ( Exception e ) {
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
             

        }
	}
	
	public boolean insert_data
	(
			String pipeline_id,
			String db_name,
			String threads,
			String batchSize,
			String totalbatchSize,
			String docSize,
			String sentences,
			String tokens,
			String lemmas,
			String ners,
			String parses,
			String dcorefs,
			String sentiments,
			String dependencies,
			String io_operations,
			String batchTimeTaken,
			String kalmanTime,
			String timeTaken,
			String rabbitmq_time,
			
			String tokenize_time,
			String ssplit_time,
			String dependency_time,
			String lemma_time,
			String ner_time,
			String parse_time,
			String dcoref_time,
			String sentiment_time, 
			String insertion_time,
			String json_object_time,
			String startup_time,
			String min_doc_size,
			String max_doc_size,
			String mean_doc_size
	)
	{
		try 
		{
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection("jdbc:sqlite:"+db_name+"_stats.db");
	        PreparedStatement P_stmt;
	        P_stmt= c.prepareStatement("INSERT INTO stats_table (pipeline_id,threads,batchSize,totalbatchSize,docsSize,sentences,tokens,lemmas,ners,parses,dcorefs,sentiments,dependencies,io_operations,batchTimeTaken,kalmanTime,timeTaken,rabbitmq_time,tokenize_time,ssplit_time,dependency_time,lemma_time,ner_time,parse_time,dcoref_time,sentiment_time,insertion_time,json_object_time,startup_time,min_doc_size,max_doc_size,mean_doc_size) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
	        P_stmt.setString(1, pipeline_id);
	        P_stmt.setString(2, threads);
	        P_stmt.setString(3, batchSize);
	        P_stmt.setString(4, totalbatchSize);
	        P_stmt.setString(5, docSize);
	        P_stmt.setString(6, sentences);
	        P_stmt.setString(7, tokens);
	        P_stmt.setString(8, lemmas);
	        P_stmt.setString(9, ners);
	        P_stmt.setString(10, parses);
	        P_stmt.setString(11, dcorefs);
	        P_stmt.setString(12, sentiments);
	        P_stmt.setString(13, dependencies);
	        P_stmt.setString(14, io_operations);
	        P_stmt.setString(15,batchTimeTaken);
	        P_stmt.setString(16,kalmanTime);
	        P_stmt.setString(17, timeTaken);
	        P_stmt.setString(18, rabbitmq_time);
	        
	        P_stmt.setString(19, tokenize_time);
	        P_stmt.setString(20, ssplit_time);
	        P_stmt.setString(21, dependency_time);
	        P_stmt.setString(22, lemma_time);
	        P_stmt.setString(23, ner_time);
	        P_stmt.setString(24, parse_time);
	        P_stmt.setString(25, dcoref_time);
	        P_stmt.setString(26, sentiment_time);
	        P_stmt.setString(27, insertion_time);
	        P_stmt.setString(28, json_object_time);
	        P_stmt.setString(29, startup_time);
	        P_stmt.setString(30, min_doc_size);
	        P_stmt.setString(31, max_doc_size);
	        P_stmt.setString(32, mean_doc_size);
	        
	        if(P_stmt.execute())
	        {
	            return true;
	        }
		} 
		catch (Exception e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
        return false;

    }
	
	public batch_info getAvgTime(String db_name)
	{
		batch_info bi = new batch_info();
		try 
		{
			bi.avgTimeTaken =-1;
			bi.avgDocSize =-1;
			
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection("jdbc:sqlite:"+db_name+"_stats.db");
			PreparedStatement P_stmt;
	        P_stmt= c.prepareStatement("SELECT AVG(batchTimeTaken),AVG(docsSize) FROM stats_table");
	       
	        ResultSet rs = P_stmt.executeQuery();
	        while(rs.next())
	        {
	        	bi.avgTimeTaken = rs.getLong(1);
	        	bi.avgDocSize = rs.getLong(2);
	        }
	        
			return  bi;
		} 
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return bi;
        
	}
	
	public batch_info getAvgTime(String db_name,String pipelineId)
	{
		batch_info bi = new batch_info();
		try 
		{
			bi.avgTimeTaken =-1;
			bi.avgDocSize =-1;
			
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection("jdbc:sqlite:"+db_name+"_stats.db");
			PreparedStatement P_stmt;
	        P_stmt= c.prepareStatement("SELECT AVG(batchTimeTaken),AVG(docsSize) FROM stats_table where pipeline_id=?");
	        P_stmt.setString(1, pipelineId);
	       
	        ResultSet rs = P_stmt.executeQuery();
	        while(rs.next())
	        {
	        	bi.avgTimeTaken = rs.getLong(1);
	        	bi.avgDocSize = rs.getLong(2);
	        }
	        
			return  bi;
		} 
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return bi;
        
	}
     
     
}
