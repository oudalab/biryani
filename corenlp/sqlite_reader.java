import org.apache.log4j.Logger;

import java.sql.*;

/**
 * Created by phani on 3/18/16.
 */
public class sqlite_reader
{
    private Logger log=Logger.getLogger(getClass());
    private Connection c;
    private PreparedStatement P_stmt;
    public int doc_present(String doc_id)
    {
        try {
            Class.forName("org.sqlite.JDBC");
            c = DriverManager.getConnection("jdbc:sqlite:test.db");
            P_stmt= c.prepareStatement("SELECT * FROM json_test_table where id=?");
            P_stmt.setString(1,doc_id);
            ResultSet rs=P_stmt.executeQuery();
            int row_count=0;
            while(rs.next())
                row_count++;
            if(row_count>0)
                return 1;
        } catch ( Exception e ) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            log.debug("Error with SQlite");

        }
        return 0;
    }
}

