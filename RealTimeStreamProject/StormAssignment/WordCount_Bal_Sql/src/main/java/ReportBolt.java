import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.sql.*; 
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import java.io.*;

public class ReportBolt extends BaseRichBolt {

	private OutputCollector collector;
    private HashMap<String, Long> ReportCounts = null;
    int temp_count_variable=0;
    private Connection con;
    private Statement stmt;
    
    // MySql connection parameters
    static final String username="root";
    static final String password="123";
    
    public void prepare(Map config, TopologyContext context, OutputCollector collector)
    {
    		this.collector = collector;
        this.ReportCounts = new HashMap<String, Long>();
        //code added to make connection to mysql database.
        try 
        {
  		Class.forName("com.mysql.jdbc.Driver");
  		con=DriverManager.getConnection("jdbc:mysql://localhost:3306/upgrad",username,password);  
  		stmt=con.createStatement(); 
        } catch ( SQLException e)
        {
        		// TODO Auto-generated catch block
        		e.printStackTrace();
        }  
        catch (ClassNotFoundException e)
        {
  		// TODO Auto-generated catch block
  		e.printStackTrace();
        }  
    }

    public void execute(Tuple tuple) 
    {
    	
        String word = tuple.getStringByField("word");
        Long count = tuple.getLongByField("count");
        
        // Aggregate the word count
        if (this.ReportCounts.get(word) != null) {
    			count = this.ReportCounts.get(word) + 1;
        }
        this.ReportCounts.put(word, count);
        
        // Ack the tuple for reliability
        this.collector.ack(tuple);
        
        temp_count_variable++;
        //Queries to insert into database
        String DeleteQuery= "DELETE FROM wordcounts";
               if(temp_count_variable==1000)
        {
        	temp_count_variable=0;
            try 
            {
            	 System.out.println("Database clearing");
				stmt.executeUpdate(DeleteQuery);
				 System.out.println("Database cleared");
			} catch (SQLException e1) {
				e1.printStackTrace();
			} 
        	List<String> keys = new ArrayList<String>();
            keys.addAll(this.ReportCounts.keySet());
            Collections.sort(keys);
            for (String key : keys)
               {
            	
            	 String InsertQuery= "INSERT INTO wordcounts(word,count) VALUES('"+key+"',"+this.ReportCounts.get(key)+")";// ON DUPLICATE KEY UPDATE count="+this.ReportCounts.get(key);    
            	 
            	 try 
            	 {
            		 System.out.println("Database Updating");
					boolean rs=stmt.execute(InsertQuery);
					 System.out.println("Database Updated");
				} catch (SQLException e)
            	 {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
         
             	 
               }
        	
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // this bolt does not emit anything
    }

    @Override
    public void cleanup() {
        System.out.println("--- FINAL COUNTS ---");
        List<String> keys = new ArrayList<String>();
        keys.addAll(this.ReportCounts.keySet());
        Collections.sort(keys);
        for (String key : keys) {
            System.out.println(key + " : " + this.ReportCounts.get(key));
        }
        System.out.println("--------------");
    }
}
