import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class WordCountTopology 
{ 
    private static final String SENTENCE_SPOUT_ID = "SentenceSpout";
    private static final String SPLIT_BOLT_ID = "SplitSentenceBolt";
    private static final String COUNT_BOLT_ID = "WordCountBolt";
    private static final String REPORT_BOLT_ID = "ReportBolt";
    private static final String TOPOLOGY_NAME = "word-count-topology";

    public static void main(String[] args) throws Exception {

        SentenceSpout spout = new SentenceSpout();
        SplitSentenceBolt splitBolt = new SplitSentenceBolt();
        WordCountBolt countBolt = new WordCountBolt();
        ReportBolt reportBolt = new ReportBolt();
  
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(SENTENCE_SPOUT_ID, spout,1);
        
        // SentenceSpout --> SplitSentenceBolt
        builder.setBolt(SPLIT_BOLT_ID, splitBolt,2)
                .shuffleGrouping(SENTENCE_SPOUT_ID);
        
        // SplitSentenceBolt --> WordCountBolt
        // Use shuffle grouping to get the load balanced among the bolts
        builder.setBolt(COUNT_BOLT_ID, countBolt,4)
                .shuffleGrouping(SPLIT_BOLT_ID);
        
        // WordCountBolt --> ReportBolt
        builder.setBolt(REPORT_BOLT_ID, reportBolt,1)
                .globalGrouping(COUNT_BOLT_ID);

        Config config = new Config();
        config.setNumAckers(1);
        if (args != null && args.length > 0)
        {
            config.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(args[0], config, builder.createTopology());
        }
        else
        {
  			
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("TwitterHashtagStorm", config, builder.createTopology());
        Thread.sleep(100000);
        cluster.shutdown();
        }

    }
}
