package BaskyUpgrad.SaavnAnalytics;

import static org.apache.spark.sql.functions.col;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

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
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import scala.collection.Seq;


public class SaavnUserClustering 
{
	public static class songMeta implements Serializable {
	    /**
		 * 
		 */
		private static final long serialVersionUID = 1L;
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
		String profilePath = "data/sample1.txt";
		//String profilePath = "data/sample100mb.csv";
		//String profilePath = "data/sampleonegb.csv";
		Dataset<Row> userProfile =
				sparkSession.read().format("csv").
				option("header","false").load(profilePath).
				toDF("UserId", "TimeStamp", "SongId", "Date");
		
		userProfile = userProfile.drop("TimeStamp", "Date");
		userProfile = userProfile.na().drop();

		//Get the song frequency per user by groupby operation		
		Dataset<Row> userRatings = 
				userProfile.groupBy("UserId", "SongId")
				.count()
				.toDF("UserId", "SongId", "Frequency");

		//Create UserIndex(Integer Type) for string UserId column to use in ALS model
		StringIndexer indexer = new StringIndexer()
				.setInputCol("UserId")
				.setOutputCol("UserIndex");

		//Table columns - UserId, SongId, Frequency, UserIndex
		Dataset<Row> userIndexed = indexer.fit(userRatings).transform(userRatings);
			
		//Create SongIndex(Integer Type) for string SongId column to use in ALS model
		indexer.setInputCol("SongId").setOutputCol("SongIndex");

		//Table columns - UserId, SongId, Frequency, UserIndex
		Dataset<Row> songIndexed =
				indexer.fit(userIndexed).transform(userIndexed);
		
		//Cast UserIndex, SongIndex to Interger Type to use in ALS model
		// <UserId,UserIndex,SongId,SongIndex,Frequency>
		Dataset<Row> modelIndexed = songIndexed
				.withColumn("UserIndex", col("UserIndex").cast(DataTypes.IntegerType))
				.withColumn("SongIndex", col("SongIndex").cast(DataTypes.IntegerType));
	
/*		modelIndexed.agg(functions.min(modelIndexed.col("UserIndex")), 
				functions.max(modelIndexed.col("UserIndex")),
				functions.min(modelIndexed.col("SongIndex")),
				functions.max(modelIndexed.col("SongIndex"))).show();
*/		
		ALS als = new ALS()
				  .setRank(10)
				  .setMaxIter(5)
				  .setRegParam(0.01)
				  .setUserCol("UserIndex")
				  .setItemCol("SongIndex")
				  .setRatingCol("Frequency");
		ALSModel model = als.fit(modelIndexed);
		
		// Get the userFactors from ALS model to use it in kmeans
		Dataset<Row> userALSFeatures = model.userFactors();

		// <UserId,UserIndex>
		Dataset<Row> userIdTable = modelIndexed
										.drop("SongIndex","SongId","Frequency")
										.groupBy("UserId","UserIndex").count().drop("count");
//		System.out.println("userIdTable schema below:");
//		userIdTable.printSchema();
		
		// <UserId,UserIndex,features(array)>
		Dataset<Row> userTableInfo = 
				userIdTable.join(userALSFeatures, userIdTable.col("UserIndex").equalTo(userALSFeatures.col("id"))).drop("id");
	
	//	System.out.println("userTableInfo schema below:");
	//	userTableInfo.printSchema();
	

		// Register Array-To-Vector converter UDF
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

		// <UserId,UserIndex,alafeatures(vector)>
		userAlsFeatureVect = userAlsFeatureVect.toDF("UserId", "UserIndex", "alsfeatures");
//		System.out.println("userAlsFeatureVect schema below:");
//		userAlsFeatureVect.printSchema();
		
		// Scale the alsfeatures before giving to kmeans
    		StandardScaler scaler = new StandardScaler()
    		  .setInputCol("alsfeatures")
    		  .setOutputCol("scaledFeatures")
    		  .setWithStd(true)
    		  .setWithMean(true);

    		// Compute summary statistics by fitting the StandardScaler
    		StandardScalerModel scalerModel = scaler.fit(userAlsFeatureVect);

    		// Normalize each feature to have unit standard deviation.
    		Dataset<Row> scaledData = scalerModel.transform(userAlsFeatureVect);
    		
  //		System.out.println("scaledData schema below:");
  //		scaledData.printSchema();
   		
    		// <UserId,UserIndex, features(vector)>
    		scaledData = scaledData.drop("alsfeatures").toDF("UserId", "UserIndex", "features");

//  		System.out.println("scaledData schema below:");
//  		scaledData.printSchema();
		
		
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
		KMeansModel modelFinal = kmeansFinal.fit(scaledData);

		/*double WSSSE = modelFinal.computeCost(scaledData);
		System.out.println("WSSSE = " + WSSSE + "For k: " + "240");*/
		
		// Make Predictions for scaled user ratings data
		Dataset<Row> usersClusterInfo = modelFinal.transform(scaledData);
		
//		usersClusterInfo.show(100, false);
//		System.out.println("usersClusterInfo schema below:");
//		usersClusterInfo.printSchema();
		//usersClusterInfo.write().csv("data/kmeans_user_cluster");
	//b	usersClusterInfo.write().save("data/kmeans_user_cluster");
		usersClusterInfo.createTempView("usersClusterInfo");

		// <cluser_id, num_of_users>
//		Dataset<Row> clusterInfo = sparkSession.sql("SELECT prediction, count(*) as noOfUsers from usersClusterInfo group by prediction");
//		System.out.println("clusterInfo schema below:");
//		clusterInfo.printSchema();

		
		userProfile = userProfile.toDF("UId", "song_id");

//		System.out.println("userProfile schema below:");
//		userProfile.printSchema();
//		System.out.println("UserProfile : Columns & Count: "+userProfile.columns().length+" : "+userProfile.count());


		// <song_id,UserId,prediction>
		Dataset<Row> userProfilePrediction = 
				userProfile.join(usersClusterInfo, userProfile.col("UId").equalTo(usersClusterInfo.col("UserId")))
				.drop("features","UId","UserIndex");

//		System.out.println("userProfilePrediction schema below:");
//		userProfilePrediction.printSchema();


		// Read the metadata to get song to artistid mapping
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
		songMetaDF = songMetaDF.na().drop();
 	//	songMetaDF.printSchema();

		// <UserId,prediction(cluserid),songId,artistIdss(array)>
		Dataset<Row> userClusterJoinSongArtistInfo =
				userProfilePrediction.join(songMetaDF, userProfilePrediction.col("song_id")
						.equalTo(songMetaDF.col("songId"))).drop("song_id");
		
	//	System.out.println("userClusterJoinSongArtistInfo with songmeta join artist schema below:");
	//	userClusterJoinSongArtistInfo.printSchema();
		
		// <UserId,prediction(cluserid),songId,artistIdss>	
		userClusterJoinSongArtistInfo =
				userClusterJoinSongArtistInfo.withColumn("artistIds", functions.explode(userClusterJoinSongArtistInfo.col("artistIds")));
		
//		System.out.println("userClusterJoinSongArtistInfo with explode artist schema below:");
//		userClusterJoinSongArtistInfo.printSchema();

		Dataset<Row> popularArtistPerCluster = 
				userClusterJoinSongArtistInfo.groupBy("prediction", "artistIds")
				.count()
				.toDF("ClusterId", "ArtistId", "Frequency");
//		System.out.println("popularArtistPerCluster schema before sql op below:");
//		popularArtistPerCluster.printSchema();
//		popularArtistPerCluster.show(100,false);
	//b	popularArtistPerCluster.write().csv("data/cluster_artist_id_freq");
		popularArtistPerCluster.createTempView("ClusterArtistFreq");
		
		// <CluserId,ArtistId,Frequency,rank>
		Dataset<Row> rankArtistPerCluster = 
				sparkSession.sql("SELECT ClusterId,ArtistId,Frequency, rank from "
						+ "(SELECT ClusterId,ArtistId,Frequency, row_number() over(partition by ClusterId order by Frequency desc) as rank"
						+ " from ClusterArtistFreq) a WHERE rank == 1 order by a.Frequency desc");
				//sparkSession.sql("SELECT * FROM ClusterArtistFreq as cf WHERE Frequency in (SELECT max('Frequency') from ClusterArtistFreq)");

//		rankArtistPerCluster.show(500,false);
		
		// Remove duplicate ArtistId assigned to multiple cluster - 1 Artistid = 1 cluser_id
		popularArtistPerCluster = rankArtistPerCluster.dropDuplicates("ArtistId");
	//b	popularArtistPerCluster.write().csv("data/popularArtistPerCluster");
		
//		System.out.println("popularArtistPerCluster schema after sql op below:");
//		popularArtistPerCluster.printSchema();

		
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

//		System.out.println("notifyData schema below:");
//		notifyData.printSchema();
		
		// Join notify data to poperArtistCluster table to get notifyId,clusterId,ArtistId table
		notifyData = 
				notifyData.join(popularArtistPerCluster,
						notifyData.col("Artist_Id").equalTo(popularArtistPerCluster.col("ArtistId")),
						"left_outer").drop("Artist_Id","Frequency","rank");
		System.out.println("notifyData schema after join popartist op below:");
	//b	notifyData.printSchema();
	//b	notifyData.show(500,false);



		
		// Not tested yet
		// <notifyId, ClusterId>
		Dataset<Row> notifyIdClusterMap = notifyData.groupBy("notifyId","ClusterId").count().drop("count");
	//b	notifyIdClusterMap.printSchema();
	//b	notifyIdClusterMap.write().csv("data/notifyClusterMap");
		
		Dataset<Row> notifyClusterUserArtistInfo =
				notifyIdClusterMap.join(userClusterJoinSongArtistInfo,
						notifyIdClusterMap.col("ClusterId").equalTo(userClusterJoinSongArtistInfo.col("prediction")),"left_outer");
	//	notifyClusterUserArtistInfo.printSchema();
		notifyClusterUserArtistInfo = notifyClusterUserArtistInfo.drop("prediction","songId");
	//	notifyClusterUserArtistInfo.printSchema();
		
		/*notifyClusterUserArtistInfo.coalesce(1).write().option("mapreduce.fileoutputcommitter.marksuccessfuljobs","false") //Avoid creating of crc files
		.option("header","true") //Write the headercsv("data/notifyCTR");
		.csv(args[0] + "/notifyClusterInfo");*/
		
		notifyClusterUserArtistInfo.repartition(notifyClusterUserArtistInfo.col("notifyId"))
			.write().option("header","true").partitionBy("notifyId").mode(SaveMode.Overwrite).csv(args[0]+ "/notifyClusterInfo");
		
		
		// <UserId, prediction>
		Dataset<Row> clusterUserMap = usersClusterInfo.drop("UserIndex","features");
	//b	clusterUserMap.printSchema();
	//b	clusterUserMap.write().csv("data/clusterUserMap");
		
		// <notifyId,ClusterId,UserId>
		Dataset<Row> notifyCluserUserMap =
				notifyIdClusterMap.join(clusterUserMap, notifyIdClusterMap.col("ClusterId")
						.equalTo(clusterUserMap.col("prediction")), "left_outer").drop("prediction");
	//b	notifyCluserUserMap.printSchema();
	//b	notifyCluserUserMap.write().csv("data/notifyCluserUserMap");
//		notifyCluserUserMap.show(300,false);
		
		Dataset<Row> notifyClusterUserSendCount = notifyCluserUserMap.groupBy("notifyId").count();
		notifyClusterUserSendCount = notifyClusterUserSendCount.toDF("notifyId","UserSendCount");
	//b	notifyClusterUserSendCount.printSchema();
//		notifyClusterUserSendCount.show(300,false);

		
		// Notification Clicks data input
		String path = "/Users/baskars/Details/NextStep/BigData/Upgrad/Project/AnalyticsAssignment/notification_clicks/";
		Dataset<Row> notify_clicks =
				sparkSession.read().format("csv").option("header","false").load(path).toDF("notify_Id","UserId","Date");
//		notify_clicks.printSchema();
//		System.out.println("notify_clicks: no of rows in table: " + notify_clicks.count());
		
		// Cleansing - Removing invalid notification id rows - <notifyId, UserId>
		notify_clicks = 
				notify_clicks.join(validNotifyId, notify_clicks.col("notify_Id").equalTo(validNotifyId.col("notifyId")),"left_outer");
//		notify_clicks.printSchema();
//		System.out.println("no of rows in table: " + notify_clicks.count());
//		notify_clicks.show(10,false);
		
		// <notifyId,UserId>
		notify_clicks = notify_clicks.na().drop().drop("notifyId","Date").toDF("notify_Id","user_Id");
//		notify_clicks.printSchema();
//		System.out.println("no of rows in table: " + notify_clicks.count());
//		notify_clicks.show(10,false);
	//b	notify_clicks.write().csv("data/notify_clicks");

		Dataset<Row> notifyMatchingUserClicks = 
				notify_clicks.join(notifyCluserUserMap, notify_clicks.col("notify_Id")
						.equalTo(notifyCluserUserMap.col("notifyId"))
						.and(notify_clicks.col("user_Id").equalTo(notifyCluserUserMap.col("UserId"))), "left_outer");
	//	notifyMatchingUserClicks.printSchema();
//		notifyMatchingUserClicks.show(300,false);
	//b	notifyMatchingUserClicks.drop("user_Id","notify_Id");
	//b	notifyMatchingUserClicks.write().csv("data/notifyMatchingClicks");
//		notifyMatchingUserClicks.printSchema();
//		notifyMatchingUserClicks.show(300,false);
		notifyMatchingUserClicks = notifyMatchingUserClicks.na().drop();
		notifyMatchingUserClicks = notifyMatchingUserClicks.groupBy("notifyId").count();
		notifyMatchingUserClicks = notifyMatchingUserClicks.toDF("notify_Id","click_cnt");
	//	notifyMatchingUserClicks.printSchema();
	//	notifyMatchingUserClicks.show(300,false);
		
		
		Dataset<Row> notifyCTR =  
				notifyClusterUserSendCount.join(notifyMatchingUserClicks, notifyClusterUserSendCount.col("notifyId")
						.equalTo(notifyMatchingUserClicks.col("notify_Id")),"left_outer").drop("notify_Id");
//		notifyCTR.printSchema();
		notifyCTR = notifyCTR.withColumn("CTR", notifyCTR.col("click_cnt").divide(notifyCTR.col("UserSendCount")));
//		notifyCTR.printSchema();
//		notifyCTR.show(300,false);
		notifyCTR.coalesce(1).write().option("mapreduce.fileoutputcommitter.marksuccessfuljobs","false") //Avoid creating of crc files
			.option("header","true") //Write the headercsv("data/notifyCTR");
			.csv(args[0] + "/notifyCTR");
		
    }
}