package BaskyUpgrad.SaavnAnalytics;

import static org.apache.spark.sql.functions.col;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import javax.sound.sampled.Line;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.ml.feature.StandardScalerModel;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import scala.collection.Seq;


public class SaavnUserClustering 
{
	public static class songMeta implements Serializable {
	    private String[] artistIds;
	    private String songId;

	    public String getSongId() {
	      return songId;
	    }   

	    public void setSongId(String sId) {
	      this.songId = sId;
	    }   

	    public String[] getArtistIds() {
	      return artistIds;
	    }   

	    public void setArtistIds(String[] aIds) {
	      this.artistIds = aIds;
	    }   
	}
	public static SparkSession sparkSession;
	
    public static void main( String[] args ) throws AnalysisException
    {
		
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		
		// setting up connection with spark
		//System.setProperty("hadoop.home.dir", "C:\\spark\\bin");
		sparkSession = SparkSession.builder()
				.appName("SaavnAnalytics")
				.master("local[*]")
				.getOrCreate();
			
		//Read user click stream data
		//String profilePath = "data/sample1.txt";
		String profilePath = "data/sample100mb.csv";
		Dataset<Row> userProfile =
				sparkSession.read().format("csv").
				option("header","false").load(profilePath).
				toDF("UserId", "TimeStamp", "SongId", "Date");
		
		userProfile.printSchema();

		String[] dropcol1 = {"TimeStamp","Date"};
		for(String col : dropcol1) {
			userProfile = userProfile.drop(col);
		}
		
	//	System.out.println("After dropping: Columns & Count: "+userProfile.columns().length+" : "+userProfile.count());
		//userProfile.show(false);
		
		userProfile = userProfile.na().drop();
	//	System.out.println("Cleaning Stage: Columns & Count: "+userProfile.columns().length+" : "+userProfile.count());
		
		//Get the song frequency per user by groupby operation		
		Dataset<Row> userRatings = 
				userProfile.groupBy("UserId", "SongId")
				.count()
				.toDF("UserId", "SongId", "Frequency");

	//	System.out.println("Groupby User,Song Count: "+userRatings.count());
		//Table columns - UserId, SongId, Frequency
		//userRatings.show(100,false);
		

	//	Dataset<Row> userIdTable = userProfile.groupBy("UserId").count();
	//	System.out.println("Unique User Count: "+userIdTable.count());
		
	//	userIdTable.show(100,false);
				
		//Create UserIndex(Integer Type) for string UserId column to use in ALS model
		StringIndexer indexer = new StringIndexer()
				.setInputCol("UserId")
				.setOutputCol("UserIndex");

		//Table columns - UserId, SongId, Frequency, UserIndex
		Dataset<Row> userIndexed = indexer.fit(userRatings).transform(userRatings);
			
		//userIndexed.show(10,false);

		//Create SongIndex(Integer Type) for string SongId column to use in ALS model
		indexer.setInputCol("SongId").setOutputCol("SongIndex");

		//Table columns - UserId, SongId, Frequency, UserIndex
		Dataset<Row> songIndexed =
				indexer.fit(userIndexed).transform(userIndexed);
		
		//songIndexed.show(10,false);
		
		//Cast UserIndex, SongIndex to Interger Type to use in ALS model
		Dataset<Row> modelIndexed = songIndexed
				.withColumn("UserIndex", col("UserIndex").cast(DataTypes.IntegerType))
				.withColumn("SongIndex", col("SongIndex").cast(DataTypes.IntegerType));
	
	//	System.out.println("Model_User_Song_Indexed row Count: "+modelIndexed.count());
	//	modelIndexed.show(100,false);
		
		modelIndexed.agg(functions.min(modelIndexed.col("UserIndex")), 
				functions.max(modelIndexed.col("UserIndex")),
				functions.min(modelIndexed.col("SongIndex")),
				functions.max(modelIndexed.col("SongIndex"))).show();
		
		ALS als = new ALS()
				  .setRank(10)
				  .setMaxIter(5)
				  .setRegParam(0.01)
				  .setUserCol("UserIndex")
				  .setItemCol("SongIndex")
				  .setRatingCol("Frequency");
		ALSModel model = als.fit(modelIndexed);
		
		Dataset<Row> userALSFeatures = model.userFactors();

	//	System.out.println("Features: Columns & Count: "+userALSFeatures.columns().length+" : "+userALSFeatures.count());
	//	System.out.println("item: "+model.getItemCol());
	//	System.out.println("item: "+ model.getUserCol());
		
		
		
	//	userALSFeatures.show(10,false);
		
	//	modelIndexed = modelIndexed.drop("SongIndex","SongId","Frequency").groupBy("UserId","UserIndex").count();
	//	modelIndexed = modelIndexed.drop("count");
	//	System.out.println("ModelIndexed filtered row Count: "+modelIndexed.count());
	//	modelIndexed.orderBy("UserIndex").show(100,false);
		Dataset<Row> userIdTable = modelIndexed
										.drop("SongIndex","SongId","Frequency")
										.groupBy("UserId","UserIndex").count().drop("count");
	//	System.out.println("userIdTable filtered row Count: "+userIdTable.count());
		System.out.println("userIdTable schema below:");
		userIdTable.printSchema();
		
		Dataset<Row> userTableInfo = 
				userIdTable.join(userALSFeatures, userIdTable.col("UserIndex").equalTo(userALSFeatures.col("id"))).drop("id");
		
	//	System.out.println(" userTableInfo after Join row Count: "+userTableInfo.count());
		System.out.println("userTableInfo schema below:");
		userTableInfo.printSchema();
		
	//	userTableInfo.show(100,false);
		
		sparkSession.udf().register("ATOV", new UDF1<Seq<Float>, Vector>() {

			private static final long serialVersionUID = 1L;

			@Override
			  public Vector call(Seq<Float> t1) {
				  List<Float> L = scala.collection.JavaConversions.seqAsJavaList(t1);
				    double[] DoubleArray = new double[t1.length()]; 
				    for (int i = 0 ; i < L.size(); i++) { 
				      DoubleArray[i]=L.get(i); 
				    } 
				    return Vectors.dense(DoubleArray); 
			  }
		}, new VectorUDT());
		
		Dataset<Row> userAlsFeatureVect = 
				userTableInfo.withColumn("featuresVect", functions.callUDF("ATOV", userTableInfo.col("features"))).drop("features");
		
		userAlsFeatureVect = userAlsFeatureVect.toDF("UserId", "UserIndex", "alsfeatures");
		System.out.println("userAlsFeatureVect schema below:");
		userAlsFeatureVect.printSchema();
		
		//userAlsFeatureVect.show(100,false);
		
		// Scale the variables
    		StandardScaler scaler = new StandardScaler()
    		  .setInputCol("alsfeatures")
    		  .setOutputCol("scaledFeatures")
    		  .setWithStd(true)
    		  .setWithMean(true);

    		// Compute summary statistics by fitting the StandardScaler
    		StandardScalerModel scalerModel = scaler.fit(userAlsFeatureVect);

    		// Normalize each feature to have unit standard deviation.
    		Dataset<Row> scaledData = scalerModel.transform(userAlsFeatureVect);
    		System.out.println("scaledData schema below:");
    		scaledData.printSchema();
  //  		scaledData.show(50, false);
    		scaledData = scaledData.drop("alsfeatures").toDF("UserId", "UserIndex", "features");
   // 		scaledData.show(50, false);
    		System.out.println("scaledData schema below:");
    		scaledData.printSchema();
		
		
		// Trains a k-means model, given array of k's
		//List<Integer> numClusters = Arrays.asList(180,200,220,240);
	   	/*List<Integer> numClusters = Arrays.asList(5);
		for (Integer k : numClusters) {
			KMeans kmeans = new KMeans().setK(k).setSeed(1L);
			KMeansModel modelk = kmeans.fit(scaledData);
		
			//Within Set Sum of Square Error (WESSE).					
			double WSSSE = modelk.computeCost(scaledData);
			System.out.println("WSSSE = " + WSSSE);
			
			//Shows the results
			
			Vector[] centers = modelk.clusterCenters();
			System.out.println("Cluster Centers for k: " + k + " ");
			for (Vector center: centers) {
			  System.out.println(center);
			}
								
		}*/
		
		KMeans kmeansFinal = new KMeans().setK(240).setSeed(1L);
	//	KMeansModel modelFinal = kmeansFinal.fit(userAlsFeatureVect);
		KMeansModel modelFinal = kmeansFinal.fit(scaledData);
		double WSSSE = modelFinal.computeCost(scaledData);
		System.out.println("WSSSE = " + WSSSE + "For k: " + "35");
		// Make Predictions
		Dataset<Row> predictionsTest = modelFinal.transform(scaledData);
//		predictionsTest.show(100, false);
		System.out.println("predictionsTest schema below:");
		predictionsTest.printSchema();
		predictionsTest.write().csv("data/kmeans_user_cluster");
		predictionsTest.createTempView("kmeansCluster");
		
		Dataset<Row> clusterInfo = sparkSession.sql("SELECT prediction, count(*) as noOfUsers from kmeansCluster group by prediction");
		System.out.println("clusterInfo schema below:");
		clusterInfo.printSchema();
		clusterInfo.show();
		
		userProfile = userProfile.toDF("UId", "song_id");
		System.out.println("userProfile schema below:");
		userProfile.printSchema();
//		System.out.println("UserProfile : Columns & Count: "+userProfile.columns().length+" : "+userProfile.count());
		Dataset<Row> userProfilePrediction = 
				userProfile.join(predictionsTest, userProfile.col("UId").equalTo(predictionsTest.col("UserId")))
				.drop("features","UId","UserIndex");
//		System.out.println("UserProfilePrediction : Columns & Count: "+userProfilePrediction.columns().length+" : "+userProfilePrediction.count());
		System.out.println("userProfilePrediction schema below:");
		userProfilePrediction.printSchema();
//		userProfilePrediction.show(100,false);

		
		String songMetaDataPath = "data/newmetadata.csv";
		JavaRDD<songMeta> songMetaRDD = sparkSession.read().textFile(songMetaDataPath).javaRDD()
				.map(line -> {
					String[] data1 = line.split(",");
					songMeta sm = new songMeta();
					sm.setSongId(data1[0]);
					sm.setArtistIds(Arrays.copyOfRange(data1, 1, data1.length));
					return sm;
				});
		Dataset<Row> songMetaDF = sparkSession.createDataFrame(songMetaRDD, songMeta.class);
		songMetaDF.printSchema();

		userProfilePrediction =
				userProfilePrediction.join(songMetaDF, userProfilePrediction.col("song_id")
						.equalTo(songMetaDF.col("songId"))).drop("song_id");
		
		System.out.println("userProfilePrediction with songmeta join artist schema below:");
		userProfilePrediction.printSchema();
	//	userProfilePrediction.show(100,false);
		
		Dataset<Row> userProfilePrediction_Join =
				userProfilePrediction.withColumn("artistIds", functions.explode(userProfilePrediction.col("artistIds")));
		System.out.println("userProfilePrediction_Join with explode artist schema below:");
		userProfilePrediction_Join.printSchema();
//		userProfilePrediction_Join.show(100,false);
		Dataset<Row> popularArtistPerCluster = 
				userProfilePrediction_Join.groupBy("prediction", "artistIds")
				.count()
				.toDF("ClusterId", "ArtistId", "Frequency");
		System.out.println("popularArtistPerCluster schema before sql op below:");
		popularArtistPerCluster.printSchema();
		popularArtistPerCluster.show(100,false);
		popularArtistPerCluster.write().csv("data/cluster_artist_id_freq.csv");
		popularArtistPerCluster.createTempView("ClusterArtistFreq");
		
		Dataset<Row> sqlDF = 
				sparkSession.sql("SELECT ClusterId,ArtistId,Frequency, rank from "
						+ "(SELECT ClusterId,ArtistId,Frequency, row_number() over(partition by ClusterId order by Frequency desc) as rank"
						+ " from ClusterArtistFreq) a WHERE rank == 1 order by a.Frequency desc");
				//sparkSession.sql("SELECT * FROM ClusterArtistFreq as cf WHERE Frequency in (SELECT max('Frequency') from ClusterArtistFreq)");
		sqlDF.show(500,false);
		popularArtistPerCluster = sqlDF.dropDuplicates("ArtistId");
		System.out.println("popularArtistPerCluster schema after sql op below:");
		popularArtistPerCluster.printSchema();
		popularArtistPerCluster.show(250,false);
	/*	sqlDF.createTempView("Clust
	 * 
	 * erArtistFreqRank");
		sqlDF = 
				sparkSession.sql("SELECT ClusterId,ArtistId,Frequency, a_rank from "
						+ "(SELECT ClusterId,ArtistId,Frequency, row_number() over(partition by ArtistId order by Frequency desc) as a_rank"
						+ " from ClusterArtistFreqRank) a WHERE a_rank < 5 order by a.ClusterId");
		//popularArtistPerCluster.groupBy("ClusterId","ArtistId","Frequency").agg(functions.max("Frequency").as("MostPop")).show();
		sqlDF.show(500,false);*/
		
		// Notification data <notifyId,ArtistId> input
		String notificationPath = "data/notification.csv";
		Dataset<Row> notifyData =
				sparkSession.read().format("csv").
				option("header","false").load(notificationPath).
				toDF("notifyId", "Artist_Id");
		
		// Cleansing the notification data
		notifyData = notifyData.na().drop();
		
		// Get unique column of valid notifyId
		Dataset<Row> validNotifyId = notifyData.drop("Artist_Id").distinct();
		System.out.println("validnotifyId no of rows:" + validNotifyId.count());
		validNotifyId.printSchema();
		
		System.out.println("notifyData schema below:");
		notifyData.printSchema();
		System.out.println("notifyData no of rows:" + notifyData.count());
		notifyData.show(320,false);
		
		// Join notify data to poperArtistCluster table to get id,c_id,artistId table
		notifyData = 
				notifyData.join(popularArtistPerCluster,
						notifyData.col("Artist_Id").equalTo(popularArtistPerCluster.col("ArtistId")),
						"left_outer").drop("Artist_Id","Frequency","rank");
		System.out.println("notifyData schema after join popartist op below:");
		notifyData.printSchema();
		System.out.println("notifyData no of rows:" + notifyData.count());
		notifyData.show(320,false);
		
		// Not tested yet
		notifyData = notifyData.groupBy("notifyId","ClusterId").count().drop("count");
		System.out.println("notifyData schema after groupby op below:");
		notifyData.printSchema();
		System.out.println("notifyData no of rows:" + notifyData.count());
		notifyData.show(320,false);
		notifyData =
				notifyData.join(clusterInfo, notifyData.col("ClusterId").equalTo(clusterInfo.col("prediction"))).drop("prediction");
		System.out.println("notifyData schema after join of cluster op below:");
		notifyData.printSchema();
		System.out.println("notifyData no of rows:" + notifyData.count());
		notifyData.show(320,false);
		
		notifyData.createTempView("notifyDataDb");
		//notifyData.groupBy("notifyId").sum("noOfUsers").as("numofusertonotify");
		
		Dataset<Row> notifyCTR =
				sparkSession.sql("SELECT notifyId, sum(noOfUsers) over(partition by notifyId) as numUserToBeNotified from notifyDataDb");
		System.out.println("notifyData schema after join of cluster op below:");
		notifyCTR.printSchema();
		System.out.println("notifyCTR no of rows:" + notifyData.count());
		notifyCTR.show(320,false);
		
		
		
		
		// Notification Clicks data input
		String path = "/Users/baskars/Details/NextStep/BigData/Upgrad/Project/AnalyticsAssignment/notification_clicks/";
		Dataset<Row> notify_clicks =
				sparkSession.read().format("csv").option("header","false").load(path).toDF("notify_Id","UserId","Date");
		notify_clicks.printSchema();
		System.out.println("notify_clicks: no of rows in table: " + notify_clicks.count());
		notify_clicks.show();
		
		// Cleansing - Removing invalid notification id rows - <notifyId, UserId>
		notify_clicks = 
				notify_clicks.join(validNotifyId, notify_clicks.col("notify_Id").equalTo(validNotifyId.col("notifyId")),"left_outer");
		notify_clicks.printSchema();
		System.out.println("no of rows in table: " + notify_clicks.count());
		notify_clicks.show(100,false);
		notify_clicks = notify_clicks.na().drop().drop("notify_Id","Date");
		System.out.println("no of rows in table: " + notify_clicks.count());
		notify_clicks.show(100,false);
		
		
		
		
    }
}