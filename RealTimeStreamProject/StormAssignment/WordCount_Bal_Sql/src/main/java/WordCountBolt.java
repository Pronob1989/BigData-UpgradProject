import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class WordCountBolt extends BaseRichBolt{
    private OutputCollector collector;

    public void prepare(Map config, TopologyContext context, 
            OutputCollector collector) {
        this.collector = collector;
    }

    // since any words can be proceed by any of the wordCount Bolt instances
    // count aggregation will be done in ReportBolt
    // so always emit the the count as 1 here
    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");

        // Do anchoring for tracking
    		this.collector.emit(tuple, new Values(word, new Long(1)));
    		
    		// Ack the tuple for reliability
        this.collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }
}
