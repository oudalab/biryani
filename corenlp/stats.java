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
            c = DriverManager.getConnection("jdbc:sqlite:"+db_name+".db");
            PreparedStatement stmt = c.prepareStatement("CREATE TABLE IF NOT EXISTS stats_table (threads VARCHAR , batchSize VARCHAR, docsSize VARCHAR,sentences VARCHAR,io_operations VARCHAR,timeTaken VARCHAR)");
            stmt.executeUpdate();
             
        } catch ( Exception e ) {
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
             

        }
	}
	public boolean insert_data(String db_name,String threads, String batchSize,String docSize,String sentences,String io_operations,String timeTaken )
	{
		try 
		{
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection("jdbc:sqlite:"+db_name+".db");
	        PreparedStatement P_stmt;
	        P_stmt= c.prepareStatement("INSERT INTO stats_table (threads,batchSize,docsSize,sentences,io_operations,timeTaken) VALUES (?,?,?,?,?,?)");
	        P_stmt.setString(1, threads);
	        P_stmt.setString(2, batchSize);
	        P_stmt.setString(3, docSize);
	        P_stmt.setString(4, sentences);
	        P_stmt.setString(5, io_operations);
	        P_stmt.setString(6,timeTaken);
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

