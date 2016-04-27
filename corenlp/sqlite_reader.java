import org.apache.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;

/**
 * Created by phani on 3/18/16.
 */
public class sqlite_reader
{
	private Logger log=Logger.getLogger(getClass());
	private Connection c;
	private PreparedStatement P_stmt;
	private ArrayList<String> mongo_id_list= new ArrayList<String>();
	public ArrayList<String> doc_present(String db_name,int size)
	{
		try {
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection("jdbc:sqlite:"+db_name+".db");
			P_stmt= c.prepareStatement("select mongo_id from json_test_table ORDER BY rowid DESC LIMIT 0,?");
			P_stmt.setString(1, String.valueOf(size));
			ResultSet rs=P_stmt.executeQuery();
			while(rs.next())
			{
				mongo_id_list.add(rs.getString(1));

			}
			return mongo_id_list;
		} catch ( Exception e ) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
			log.debug("Error with SQlite");

		}
		return mongo_id_list;
	}
	public boolean insert_batch_info(String db_name,String batch_id,String batch_size,String data_size,String sentences,long batch_time)
	{
		try {
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection("jdbc:sqlite:"+db_name+".db");
			P_stmt= c.prepareStatement("INSERT INTO batch_info (batch_id,batch_size,data_size,sentences,batch_time) VALUES (?,?,?,?,?)");
			P_stmt.setString(1, batch_id);
			P_stmt.setString(2, batch_size);
			P_stmt.setString(3, data_size);
			P_stmt.setString(4, sentences);
			P_stmt.setLong(5, batch_time);
			if(P_stmt.execute())
			{
				return true;
			}
			 
		} 
		catch ( Exception e ) 
		{
			e.printStackTrace();
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
			log.debug("Error with SQlite");
			return false;

		}
		return false;

	}
}
