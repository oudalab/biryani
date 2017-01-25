import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;


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
            		+ "json_object_time ,"
            		+"startup_time"
            		+ ")"
            		
            		);
            
            stmt.executeUpdate();
             
        } catch ( Exception e ) {
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
             

        }
	}
	
	public boolean insert_data
	(
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
			String startup_time
	)
	{
		try 
		{
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection("jdbc:sqlite:"+db_name+"_stats.db");
	        PreparedStatement P_stmt;
	        P_stmt= c.prepareStatement("INSERT INTO stats_table (threads,batchSize,totalbatchSize,docsSize,sentences,tokens,lemmas,ners,parses,dcorefs,sentiments,dependencies,io_operations,timeTaken,rabbitmq_time,tokenize_time,ssplit_time,dependency_time,lemma_time,ner_time,parse_time,dcoref_time,sentiment_time,insertion_time,json_object_time,startup_time) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
	        P_stmt.setString(1, threads);
	        P_stmt.setString(2, batchSize);
	        P_stmt.setString(3, totalbatchSize);
	        P_stmt.setString(4, docSize);
	        P_stmt.setString(5, sentences);
	        P_stmt.setString(6, tokens);
	        P_stmt.setString(7, lemmas);
	        P_stmt.setString(8, ners);
	        P_stmt.setString(9, parses);
	        P_stmt.setString(10, dcorefs);
	        P_stmt.setString(11, sentiments);
	        P_stmt.setString(12, dependencies);
	        P_stmt.setString(13, io_operations);
	        P_stmt.setString(14, timeTaken);
	        P_stmt.setString(15, rabbitmq_time);
	        
	        P_stmt.setString(16, tokenize_time);
	        P_stmt.setString(17, ssplit_time);
	        P_stmt.setString(18, dependency_time);
	        P_stmt.setString(19, lemma_time);
	        P_stmt.setString(20, ner_time);
	        P_stmt.setString(21, parse_time);
	        P_stmt.setString(22, dcoref_time);
	        P_stmt.setString(23, sentiment_time);
	        P_stmt.setString(24, insertion_time);
	        P_stmt.setString(25, json_object_time);
	        P_stmt.setString(26, startup_time);
	        
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
     
     
}
