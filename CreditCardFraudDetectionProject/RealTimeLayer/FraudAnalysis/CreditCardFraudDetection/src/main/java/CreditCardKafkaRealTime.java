

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Vector;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

class ColumnData implements java.io.Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private String colFamily;
	private String colName;
	private String value;
	
	ColumnData(String colF, String colN) {
		this.colFamily = colF;
		this.colName = colN;
		value = null;
	}
	ColumnData(String colF, String colN, String val) {
		this.colFamily = colF;
		this.colName = colN;
		value = val;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	public String getColName() {
		return colName;
	}
	public void setColName(String colName) {
		this.colName = colName;
	}
	public String getColFamily() {
		return colFamily;
	}
	public void setColFamily(String colFamily) {
		this.colFamily = colFamily;
	}

	
}

class RowData implements java.io.Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private HTable table;
	private byte[] rowKey;
	private Vector<ColumnData> colInfo;
	
	public RowData(HTable t, byte[] bytes) {
		// TODO Auto-generated constructor stub
		this.table = t;
		this.rowKey = bytes;
		this.colInfo = new Vector<ColumnData>();
	}
	public byte[] getRowKey() {
		return rowKey;
	}
	public void setRowKey(byte[] rowKey) {
		this.rowKey = rowKey;
	}

	public Vector<ColumnData> getColInfo() {
		return colInfo;
	}
	
	public void addColInfo(ColumnData col) {
		this.colInfo.add(col);
	}
	public void removeColInfo(ColumnData col) {
		if (this.colInfo.contains(col)) {
			this.colInfo.remove(col);
		}
	}

	public HTable getTable() {
		return table;
	}

	public void setTable(HTable table) {
		this.table = table;
	}
	
}

//Utility class to store the parsed data from JSON
class CreditCardPOS implements java.io.Serializable {
	/**
	 * 
     */
	private static final long serialVersionUID = 1L;
	
	private Long cardId;
	private Long memberId;
	private Integer amount;
	private Integer postCode;
	private Long postId;
	private String transDate;
	private String status;
	
	public CreditCardPOS() {
	
	}
	
	public Long getCardId() {
		return cardId;
	}
	public void setCardId(Long cid) {
		cardId = cid;
	}
	
	public void setTransDate(String tmp) {
		transDate = tmp;
	}
	
	public String getTransDate() {
		return transDate;
	}
	
	public void setMemberId(Long mid) {
		memberId = mid;
	}
	
	public Long getMemberId() {
		return memberId;
	}
	
	public void setAmount(Integer amt) {
		amount = amt;
	}
	
	public Integer getAmount() {
		return amount;
	}
	
	public Integer getPostCode() {
		return postCode;
	}
	
	public void setPostCode(Integer pc) {
		postCode = pc;
	}
	public void setPostId(Long pid) {
		postId = pid;
	}
	public Long getPostId() {
		return postId;
	}
	
	public String toString() {
		return (cardId + " " + memberId + " " + amount + " " + postCode + " " +"\n"
				+ "\t" + postId +  " " +  transDate + " " + status);
		
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}
}


public class CreditCardKafkaRealTime implements java.io.Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final long memberScoreThreshold = 200;
	private static final double memberSpeedThreshold = 0.25;
	
	static HTable lookupTable = null;
	static HTable cardTransTable = null;

	public static void main(String[] args) throws InterruptedException, IOException {
		// TODO Auto-generated method stub
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        if (args.length != 3) {
        		System.out.println(
        				"Usage: spark2-submit --master yarn --deploy-mode client"
        				+ "CreditCardFraudDetection-0.0.1-SNAPSHOT.jar zipCodePosId.csv hbaseDbIp kafkagroupid");
        		return;
        }
		
		// Create Spark Conf
        SparkConf sparkConf =
        		new SparkConf().setAppName("CreditCardFraudDetection").setMaster("local");
        
        // Streaming Context Creation for 1sec Duration
        JavaStreamingContext jssc =
        		new JavaStreamingContext(sparkConf, Durations.seconds(1));
        
        // Initialize Distance Utility DB
		try {
			DistanceUtility.InitDistanceUtility(args[0]);
		} catch (NumberFormatException | IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		// Create Hbase Table Connections
		CreditCardKafkaRealTime.lookupTable =
				HbaseConnection.GetHbaseTableConnections(args[1], "lookup");
		CreditCardKafkaRealTime.cardTransTable =
				HbaseConnection.GetHbaseTableConnections(args[1], "card_transactions");
		
		System.out.println("Connect with kafka : " );
        // Kafka Conf
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "100.24.223.181:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", args[2]);
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", true);

        // Topic Detail
        Collection<String> topics = Arrays.asList("transactions-topic-verified");
        
        // Kafka Dstream Creation
        JavaInputDStream<ConsumerRecord<String, String>> kstream = 
        		KafkaUtils.createDirectStream(jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

        JavaDStream<String> customerPOS = kstream.map(x -> x.value());

        customerPOS.foreachRDD(x -> System.out.println("Stream POS Count: " + x.count()));
        
		// Convert whole string to individual line of JSON object
		JavaDStream<Object> card_trans_json = customerPOS.flatMap(new FlatMapFunction<String, Object>() {

			private static final long serialVersionUID = 1L;

			@SuppressWarnings("unchecked")
			public Iterator<Object> call(String line) throws ParseException {
			JSONParser jParser = new JSONParser();
			Object obj  = jParser.parse(line);
			JSONArray jarray = new JSONArray();
			jarray.add(obj);
			return jarray.iterator();
			}
		});
		
		// Get individual CreditCardPOS data from JSON object
		JavaDStream<CreditCardPOS> card_pos = card_trans_json.map(new Function<Object, CreditCardPOS>() {

			private static final long serialVersionUID = 1L;

			public CreditCardPOS call(Object obj) {
				JSONObject jobj = (JSONObject)obj;
				CreditCardPOS cd = new CreditCardPOS();
				cd.setCardId(Long.parseLong(jobj.get("card_id").toString()));
				cd.setMemberId(Long.parseLong(jobj.get("member_id").toString()));
				cd.setAmount(Integer.parseInt(jobj.get("amount").toString()));
				cd.setPostCode(Integer.parseInt(jobj.get("postcode").toString()));
				cd.setPostId(Long.parseLong(jobj.get("pos_id").toString()));
				cd.setTransDate(jobj.get("transaction_dt").toString());
				return cd;				
			}
		});
	
		// Determine whether card transaction is GENINUE/FRAUD
		card_pos.foreachRDD(new VoidFunction<JavaRDD<CreditCardPOS>>() {

			private static final long serialVersionUID = 1L;

			public void call(JavaRDD<CreditCardPOS> rdd) throws IOException {

        			rdd.foreach(a -> {
        				boolean state = true;
        				
        				// Get ucl, score, pc & tdt info from lookup table
        				RowData row = 
        						new RowData(CreditCardKafkaRealTime.lookupTable,
        								Bytes.toBytes(a.getCardId().toString()));
        				ColumnData colScore = new ColumnData("bt", "score");
        				ColumnData colUcl = new ColumnData("bt", "ucl");
        				ColumnData colPc = new ColumnData("st", "pc");
        				ColumnData colTdt = new ColumnData("st", "tdt");
        				
        				row.addColInfo(colScore);
        				row.addColInfo(colUcl);
        				row.addColInfo(colPc);
        				row.addColInfo(colTdt);
        				
        				
        				boolean status = HbaseDAO.getRowData(row);
        				if (status == false) {
        					//Not a valid card member id make it FRAUD transaction
        					System.out.println("Not a valid card member " + a);
        					state = false;
        					
        				} else if ((Integer.parseInt(colScore.getValue()) == 0) && 
        						(Integer.parseInt(colUcl.getValue()) == 0) && 
        						(Integer.parseInt(colPc.getValue()) == 0)) {
        					//First transactions for new card member so declare it GENUINE
        					state = true;
        					
        				} else {
        					// Check the memscore & UCL threshold
        					if ((Integer.parseInt(colScore.getValue()) < memberScoreThreshold)) {
        						state = false;
        						System.out.println("Failed Score: ts " + Integer.parseInt(colScore.getValue()) + " ls: " + memberScoreThreshold);
        					}
        					
        					if (state && a.getAmount() > Integer.parseInt(colUcl.getValue())) {
        						state = false;
        						System.out.println(
        								"Failed UCL: tamt: " + a.getAmount() + " lamt: " + Integer.parseInt(colUcl.getValue()));
        					}
        					if (state) {
	        					Integer tPc = a.getPostCode();
	        					//Calc distance traveled btw prev and current transactions
	        					Double dist = 
	        							DistanceUtility.getDistanceViaZipCode(
	        								colPc.getValue(), tPc.toString());
	        					Date start = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss", Locale.ENGLISH)
	        		                    .parse(colTdt.getValue());
	        		            Date end = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss", Locale.ENGLISH)
	        		                    .parse(a.getTransDate());
	        		            Double diffTimeSec = (double) (Math.abs(end.getTime()-start.getTime())/1000);
	        		            
	        					Double speed = dist/diffTimeSec;
	        					if (speed > memberSpeedThreshold) {
	        						state = false;
	        						System.out.println("Failed Dist: ts " + speed + " ls: " + memberSpeedThreshold);
	        					}
        					}
        				}
    					if (state) {
    						a.setStatus("GENUINE");
    						System.out.println("Trans state is GENUINE for : " + a);
    						
    						row.removeColInfo(colUcl);
    						row.removeColInfo(colScore);
    						colPc.setValue(a.getPostCode().toString());
    						colTdt.setValue(a.getTransDate());
    						// Update the lookup table with postcode & date for GENUINE transactions
    						boolean statusl = HbaseDAO.putRowData(row);
    						if (!statusl)
    							System.out.println("lookup update failed for rowkey: " + row.getRowKey().toString());
    						
    					} else {
    						a.setStatus("FRAUD");
    						System.out.println("Trans state is FRAUD for : " + a);
    					}

    					String StartOfText = "\u0002";
    					byte[] rowK = Bytes.toBytes(
    							(a.getMemberId().toString() + StartOfText + a.getTransDate() + StartOfText + a.getAmount().toString()));
    
        				// Update the card_trans table
        				RowData rowT = new RowData(CreditCardKafkaRealTime.cardTransTable, rowK);
        				
        				ColumnData colCardIdT = new ColumnData("md", "cid", a.getCardId().toString());
        				ColumnData colPosT = new ColumnData("td", "pos", a.getPostId().toString());
        				ColumnData colPcT = new ColumnData("td", "pc", a.getPostCode().toString());
        				ColumnData colStatusT = new ColumnData("td", "st", a.getStatus());
        				
        				rowT.addColInfo(colCardIdT);
        				rowT.addColInfo(colPosT);
        				rowT.addColInfo(colPcT);
        				rowT.addColInfo(colStatusT);
        				
        				HbaseDAO.putRowData(rowT);
        			});
            }
        });
		
        jssc.start();
        // / Add Await Termination to respond to Ctrl+C and gracefully close Spark
        // Streams
        try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			System.out.println("Closing table connection ");
			lookupTable.close();
			cardTransTable.close();
			HbaseConnection.hbaseAdmin.close();
			e.printStackTrace();
		}
	}

}
