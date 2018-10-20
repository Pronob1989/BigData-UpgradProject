import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class SentenceSpout extends BaseRichSpout {

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private SpoutOutputCollector collector;
    private String[] sentences = {
        "my dog has fleas",
        "i like cold beverages",
        "the dog ate my homework",
        "dont have a cow man",
        "i dont think i like fleas"
    };
    private int index = 0;
    
    // Map of (msgId, sentence)
    private HashMap<String, String> msg_db = null;
    
    public void declareOutputFields(OutputFieldsDeclarer declarer) 
    {
        declarer.declare(new Fields("sentence"));
    }

    public void open(Map config, TopologyContext context, 
            SpoutOutputCollector collector) 
    {
        this.collector = collector;
        this.msg_db = new HashMap<String, String>();
    }

    public void nextTuple() 
    {
    		String msgId = UUID.randomUUID().toString();
    		this.collector.emit(new Values(sentences[index]), msgId);
    		msg_db.put(msgId, sentences[index]);
    		index++;
    		if (index >= sentences.length) 
    		{
    			index = 0;
    		}

    }
    public void ack(Object msgId) {
    		// Remove the ack msg from msg_db
	 	msg_db.remove(msgId);
    }
    public void fail(Object msgId) {
    		// Retransmit the failed msg using the msg_db
	 	this.collector.emit(new Values(msg_db.get(msgId)), msgId);
 	}
}