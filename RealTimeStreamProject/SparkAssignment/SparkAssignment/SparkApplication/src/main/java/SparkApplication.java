
import java.util.Date;
import java.util.Iterator;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import scala.Tuple2;


// Utility class to store the parsed data from JSON
class Stock implements java.io.Serializable {
/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
private String symbol;
private String timestamp;
private double high;
private double close;
private double open;
private double low;
private int volume;

public Stock(String sym, String time) {
	symbol = sym;
	timestamp = time;
}
public Stock() {

}

public String getSymbol() {
	return symbol;
}
public void setSymbol(String sym) {
	symbol = sym;
}

public void setTimeStamp(String tmp) {
	timestamp = tmp;
}

public void setHigh(double val) {
	high = val;
}

public double getClose() {
	return close;
}

public void setClose(double val) {
	close = val;
}

public double getOpen() {
	return open;
}

public void setOpen(double val) {
	open = val;
}
public void setLow(double val) {
	low = val;
}
public int getVolume() {
	return volume;
}
public void setVolume(int val) {
	volume = val;
}


public String toString() {
	return (symbol + " " + timestamp + " " + high + " " + low + " " +"\n"
			+ "\t" + open +  " " + close + " " + volume);
	
}
}

// Utility class to store data for Average calculation
class StockAvgInfo implements java.io.Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private double open;
	private double close;
	private int count;
	public StockAvgInfo(int cnt, double cl, double opn) {
		count = cnt;
		close = cl;
		open = opn;
	}
	public StockAvgInfo(int cnt, double cl) {
		count = cnt;
		close = cl;
	}
	public double getClose() {
		return close;
	}
	public double getOpen() {
		return open;
	}
	public int getCount() {
		return count;
	}
	public String toString() {
		return ("count, close, open " + " " + count + " " + close + open);
		
	}
}

// Utility class to store data for RS calculation
class StockProfitInfo implements java.io.Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private double profit;
	private double loss;
	private int count;
	public StockProfitInfo(int cnt, double pt, double ls) {
		profit = pt;
		loss = ls;
		count = cnt;
	}
	public double getProfit() {
		return profit;
	}
	public double getLoss() {
		return loss;
	}
	public int getCount() {
		return count;
	}
	public String toString() {
		return ("count, profit, loss " + " " + count + " " + profit + loss);
		
	}
}

public class SparkApplication {

	public static void main(String[] args) throws InterruptedException {
		
		if (args.length != 2) {
			System.out.println(
				"Usage: java -cp SparkApplication-0.0.1-SNAPSHOT.jar SparkApplication input_stream_dir output_dir");
			return;
		}
		
		// Setup context use 2 cores - 1 for receiving stream data 1 for processing
		// checkpoint and logging parameters
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("FirstSparkApplication");
		
		// Set Batch Interval to 1minute
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.minutes(1));
		
		// Use output directory as checkpoint directory
		jssc.checkpoint(args[1] + "/stream_checkpoint");
		
		// Set Error logging
		Logger.getRootLogger().setLevel(Level.ERROR);
		
		// Read input stream from directory
		JavaDStream<String> stock_text = jssc.textFileStream(args[0]);
		
		// Convert whole string to individual line of JSON object
		JavaDStream<Object> all_stock = stock_text.flatMap(new FlatMapFunction<String, Object>() {

			private static final long serialVersionUID = 1L;

			@SuppressWarnings("unchecked")
			public Iterator<Object> call(String line) throws ParseException {
			JSONParser jParser = new JSONParser();
			JSONArray jArray = (JSONArray) jParser.parse(line);
			return jArray.iterator();
			}
		});
		
		// Get individual stock data from JSON object
		JavaDStream<Stock> stocks = all_stock.map(new Function<Object, Stock>() {

			private static final long serialVersionUID = 1L;

			public Stock call(Object obj) {
				JSONObject jobj = (JSONObject)obj;
				Stock st = new Stock();
				st.setSymbol((String)jobj.get("symbol"));
				st.setTimeStamp((String)jobj.get("timestamp"));
				JSONObject jp = (JSONObject)jobj.get("priceData");
				st.setHigh(Double.parseDouble((String)(jp.get("high"))));
				st.setLow(Double.parseDouble((String)(jp.get("low"))));
				st.setOpen(Double.parseDouble((String)(jp.get("open"))));
				st.setClose(Double.parseDouble((String)(jp.get("close"))));
				st.setVolume(Integer.parseInt((String)(jp.get("volume"))));
				return st;				
			}
		});
		
		// Get the aggregate stock data for 10min Window and 5min Sliding window Interval
		JavaPairDStream<String, StockAvgInfo> stock_window_agg_data = 
			stocks.mapToPair(stock -> 
				new Tuple2<>(stock.getSymbol(), new StockAvgInfo(1, stock.getClose(), stock.getOpen())))
			.reduceByKeyAndWindow((sma_x, sma_y) ->
				new StockAvgInfo(sma_x.getCount() + sma_y.getCount(),
				sma_x.getClose() + sma_y.getClose(),
				sma_x.getOpen() + sma_y.getOpen()),
				Durations.minutes(10), Durations.minutes(5));
		
		
		// Problem 1: Find Simple moving average for aggregate data (10min Window and 5min Sliding window)
		JavaPairDStream<String,Double> stock_avg_close_price =
			stock_window_agg_data.mapToPair(info ->
				new Tuple2<>(info._1, info._2.getClose()/info._2.getCount()));
		
		stock_avg_close_price.foreachRDD(rdd ->{
	          if(!rdd.isEmpty()){
	        	  	rdd.coalesce(1).saveAsTextFile(args[1] + "/prob1_avg_close" + new Date().getTime());
	          }
	      });
		
		
		// Problem 2: Find Max profit stock for aggregate data (10min Window and 5min Sliding window)
		JavaPairDStream<String,Double> stock_profit_info =
			stock_window_agg_data.mapToPair(info ->
				//Max profit by subtracting average close & open per stock
				new Tuple2<>(info._1,
						((info._2.getClose()/info._2.getCount()) -
								(info._2.getOpen()/info._2.getCount()))));
		
		JavaPairDStream<Double, String> stocks_max_profit = 
			stock_profit_info.mapToPair(
					//Exchange key/value for sorting based on max_profit value
					x -> new Tuple2<Double, String>(x._2, x._1))
					.transformToPair(s -> s.sortByKey(false)); 
		
		stocks_max_profit.foreachRDD(rdd ->{
	          if(!rdd.isEmpty()){
	             rdd.coalesce(1).saveAsTextFile(args[1] + "/prob2_max_profit" + new Date().getTime());
	          }
	      });
		
		// Problem 3: Find RSI of stocks data (10min Window and 5min Sliding window)		
		JavaPairDStream<String, StockProfitInfo> stocks_profit = 
			stocks.mapToPair(
				new PairFunction<Stock, String, StockProfitInfo>() {

				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<String, StockProfitInfo> call(Stock sk) throws Exception {
					// Get stock profit/loss value for each stock
					double stock_profit = sk.getClose() - sk.getOpen();
					if (stock_profit > 0) {
						return new Tuple2<>(sk.getSymbol(), new StockProfitInfo(1, stock_profit,0)); 
					} else if (stock_profit < 0) {
						return new Tuple2<>(sk.getSymbol(), new StockProfitInfo(1,0,stock_profit));
					} else {
						return new Tuple2<>(sk.getSymbol(), new StockProfitInfo(1,0,0));
					}
				}
			})
			.reduceByKeyAndWindow(
				//Aggregated profit & loss values for each stock on this window
				(stk_profit_x, stk_profit_y) -> 
					new StockProfitInfo(
						stk_profit_x.getCount() + stk_profit_y.getCount(),
						stk_profit_x.getProfit() + stk_profit_y.getProfit(),
						stk_profit_x.getLoss() + stk_profit_y.getLoss()),
					Durations.minutes(10), Durations.minutes(5))
			.mapToPair(stk -> 
				//Get Average Gain/Loss for each stock on this window
				new Tuple2<>(stk._1,
						new StockProfitInfo(stk._2.getCount(),
								stk._2.getProfit()/stk._2.getCount(),
								stk._2.getLoss()/stk._2.getCount())));
			
		JavaPairDStream<String, Double> stocks_rsi =
			stocks_profit.mapToPair(new PairFunction<Tuple2<String, StockProfitInfo>, String, Double> ()
					{

						private static final long serialVersionUID = 1L;

						@Override
						public Tuple2<String, Double> call(Tuple2<String, StockProfitInfo> stk)
								throws Exception {
							// Calculate RSI for each stock
							double profit = Math.abs(stk._2.getProfit());
							double loss = Math.abs(stk._2.getLoss());
							double rsi = 0;
							if (loss == 0) {
								// No loss so make RSI as 100
								rsi = 100; 
							} else {
								//If profit is 0 then rsi = 100
								double rs = profit/loss;
								rsi = (100 - (100/(1+ rs)));
							}
							// TODO Auto-generated method stub
							return new Tuple2<String, Double>(stk._1, rsi);
						}
				
					});
				
		stocks_rsi.foreachRDD(rdd ->{
	          if(!rdd.isEmpty()){
	             rdd.coalesce(1).saveAsTextFile(args[1] + "/prob3_stocks_rsi" + new Date().getTime());
	          }
	      });
				
		// Problem 4: Find Max volume stock for window data (10min Window and 5min Sliding window)		
		JavaPairDStream<String, Integer> stocks_volume = 
			stocks.mapToPair(stock -> new Tuple2<>(stock.getSymbol(), stock.getVolume()))
			.reduceByKeyAndWindow(
				//Aggregate volume for each stock
				(vol_x, vol_y) -> vol_x + vol_y,
				Durations.minutes(10), Durations.minutes(5));
		
		JavaPairDStream<Integer, String> stocks_vol_sort = 
			stocks_volume.mapToPair(
				// Exchange key/value for volume based sorting (descending order)
				x -> new Tuple2<Integer, String>(x._2, x._1))
				.transformToPair(s -> s.sortByKey(false)); 
		
		stocks_vol_sort.foreachRDD(rdd ->{
	          if(!rdd.isEmpty()){
	             rdd.coalesce(1).saveAsTextFile(args[1] + "/prob4_max_vol" + new Date().getTime());
	          }
	      });
		
		
		// Start and termination of App
		jssc.start();
		jssc.awaitTermination();
		jssc.close();

	}

}
