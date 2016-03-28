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
    public ArrayList<String> doc_present(int size)
    {
        try {
            Class.forName("org.sqlite.JDBC");
            c = DriverManager.getConnection("jdbc:sqlite:test.db");
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
}
