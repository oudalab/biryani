import org.apache.log4j.Logger;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;

public class sqlite_reader
{
    private Logger log=Logger.getLogger(getClass());
    private Connection c;
    private Statement stmt;
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
    public boolean insert_batch_info(String db_name,String batch_id,Integer batch_size,Integer data_size,Integer sentences,Integer batch_time)
    {
        try {
            Class.forName("org.sqlite.JDBC");
            c = DriverManager.getConnection("jdbc:sqlite:"+db_name+".db");
            P_stmt= c.prepareStatement("INSERT INTO batch_info (batch_id,batch_size,data_size,sentences,batch_time) VALUES (?,?,?,?,?)");
            P_stmt.setString(1, batch_id);
            P_stmt.setInt(2, batch_size);
            P_stmt.setInt(3, data_size);
            P_stmt.setInt(4, sentences);
            P_stmt.setInt(5, batch_time);
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
    public HashMap<String, Float> get_avg_info(String db_name)
    {
    	HashMap<String , Float> data= new HashMap<String, Float>();
    	try {
            Class.forName("org.sqlite.JDBC");
            c = DriverManager.getConnection("jdbc:sqlite:"+db_name+".db");
            stmt=c.createStatement();
            ResultSet rs=stmt.executeQuery("Select round(avg(data_size)),round(avg(batch_time)) from batch_info");
            while(rs.next())
            {
            	data.put("avg_size", rs.getFloat(1));
            	data.put("avg_time", rs.getFloat(2));

            }
        } 
    	catch ( Exception e ) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            log.debug("Error with SQlite");

        }
    	
    	return data;
    }
}
