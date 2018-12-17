import static org.apache.spark.sql.functions.col;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

public class TwitterGenderClassification {

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);

		if (args.length != 1) {
			System.out.println(
				"Usage: spark-submit --master local TwitterClassify-0.0.1-SNAPSHOT.jar "
			 	+ "input_dir_path"); 
			return;
		}
		
		SparkSession sparkSession = SparkSession.builder()  //SparkSession  
				.appName("SparkML") 
				.master("local[*]") 
				.getOrCreate(); //

		// Read the file as a training dataset
		String pathTrain = args[0];
		Dataset<Row> dataset = sparkSession.read()
				.option("header","true")
				.option("inferSchema",true)
				.option("escape", "\"")
				.option("mode", "DROPMALFORMED")
				.csv(pathTrain);
		
		System.out.println("Num of rows read: " + dataset.count());
		
		//Isolate the relevant columns
		Dataset<Row> cleandata = 
				dataset.select(dataset.col("gender"), dataset.col("text"),
						dataset.col("fav_number").cast(DataTypes.IntegerType),
						dataset.col("created"),
						dataset.col("tweet_count").cast(DataTypes.IntegerType))
				.filter(dataset.col("gender").equalTo("male")
						.or(dataset.col("gender").equalTo("female"))
						.or(dataset.col("gender").equalTo("brand")));
		
		cleandata.printSchema();

		// Drop rows with null values
		cleandata = cleandata.na().drop();

		System.out.println("Num of valid data: " + cleandata.count());
		
		// Create new Column "NoOfDays" by taking diff of account creation date to present date
		cleandata = cleandata.withColumn(
				"NoOfDays", functions.datediff(functions.current_date(),
				functions.to_date(functions.unix_timestamp(cleandata.col("created"),
				"MM/dd/yy").cast(DataTypes.TimestampType))));
		
		// UDF: Check for http link in the string and return true if found else false 
		sparkSession.udf().register("HttpText", new UDF1<String, Boolean>() {

			private static final long serialVersionUID = 1L;

			@Override
			  public Boolean call(String t1) throws Exception{
				return t1.contains("https");
			  }
		}, DataTypes.BooleanType);
		
		// Create column "HttpLink" to indicate whether text column contain link data or not
		cleandata = cleandata.withColumn("HttpLink", functions.callUDF("HttpText", cleandata.col("text")));
		
		// UDF: Calculate Tweet count per day by doing tweetCnt/noOfDays of input params
		sparkSession.udf().register("TweetCntData", new UDF2<Integer, Integer, Double>() {

			private static final long serialVersionUID = 1L;

			@Override
			  public Double call(Integer noOfDays, Integer tweetCnt) throws Exception{
				if (noOfDays > 0) {
					return (double) (tweetCnt/noOfDays);
				}
				return 0.0;
			  }
		}, DataTypes.DoubleType);
		
		// Create column "TweetCntPerDay" to indicate tweets per day for each user
		cleandata = cleandata.withColumn("TweetCntPerDay",
				functions.callUDF("TweetCntData", cleandata.col("NoOfDays"), cleandata.col("tweet_count")));
		
		// Split the data for training and test
		Dataset<Row>[] splits = cleandata.randomSplit(new double[]{0.7, 0.3},30L);
        Dataset<Row> traindata = splits[0];		//Training Data
        Dataset<Row> testdata = splits[1];		//Testing Data*/
        
        System.out.println("Num of valid train data: " + traindata.count());
        traindata.groupBy(cleandata.col("gender")).count().show();
        System.out.println("Num of valid test data: " + testdata.count());
        testdata.groupBy(cleandata.col("gender")).count().show();       
		
		// Configure an ML pipeline, which consists of multiple stages:
        // 	indexer, tokenizer, hashingTF, idf, lr/rf etc 
		//  and labelindexer.
        
		//Relabel the target variable
		StringIndexerModel labelindexer = new StringIndexer()
				.setInputCol("gender")
				.setOutputCol("label").fit(traindata);

		// Tokenize the input text
		Tokenizer tokenizer = new Tokenizer()
				.setInputCol("text")
				.setOutputCol("words");

		// Remove the stop words
		StopWordsRemover remover = new StopWordsRemover()
				.setInputCol(tokenizer.getOutputCol())
				.setOutputCol("filtered");		

		// Create the Term Frequency Matrix
		HashingTF hashingTF = new HashingTF()
				.setNumFeatures(1000)
				.setInputCol(remover.getOutputCol())
				.setOutputCol("numFeatures");

		// Calculate the Inverse Document Frequency 
		IDF idf = new IDF()
				.setInputCol(hashingTF.getOutputCol())
				.setOutputCol("IDFfeatures");

        //Assembling the features in the dataFrame as Dense Vector
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{"IDFfeatures","HttpLink","fav_number","TweetCntPerDay"})
                .setOutputCol("features");
        
		// Set up the Random Forest Model
		RandomForestClassifier rf = new RandomForestClassifier().setMaxDepth(10);

		//Set up Decision Tree
		DecisionTreeClassifier dt = new DecisionTreeClassifier().setMaxDepth(10);

		// Convert indexed labels back to original labels once prediction is available	
		IndexToString labelConverter = new IndexToString()
				.setInputCol("prediction")
				.setOutputCol("predictedLabel").setLabels(labelindexer.labels());

		// Create and Run Random Forest Pipeline
		Pipeline pipelineRF = new Pipeline()
				.setStages(new PipelineStage[] {labelindexer, tokenizer, remover, hashingTF, idf, assembler, rf,labelConverter});	
		
		// Fit the pipeline to training documents.
		PipelineModel modelRF = pipelineRF.fit(traindata);		
		
		// Make predictions on test documents.
		Dataset<Row> predictionsTestDataRF = modelRF.transform(testdata);
		
	//	System.out.println("Predictions from Random Forest Model are:");
	//	predictionsTestDataRF.show(10);
		
		// Make predictions on train documents
		Dataset<Row> predictionsTrainDataRF = modelRF.transform(traindata);

		// Create and Run Decision Tree Pipeline
		Pipeline pipelineDT = new Pipeline()
				.setStages(new PipelineStage[] {labelindexer, tokenizer, remover, hashingTF, idf, assembler, dt,labelConverter});	
		
		// Fit the pipeline to training documents.
		PipelineModel modelDT = pipelineDT.fit(traindata);		
		
		// Make predictions on test documents.
		Dataset<Row> predictionsDT = modelDT.transform(testdata);

	//	System.out.println("Predictions from Decision Tree Model are:");
	//	predictionsDT.show(10);		

		// Make predictions on training documents.
		Dataset<Row> predictionsTrainDT = modelDT.transform(traindata);
		
		// Select (prediction, true label) and compute test error.
		MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
				.setLabelCol("label")
				.setPredictionCol("prediction")
				.setMetricName("accuracy");	

		
		//Evaluate Accuracy for Random Forest
		double accuracyRF = evaluator.evaluate(predictionsTestDataRF);
		System.out.println("Test Accuracy for Random Forest		= " + (accuracyRF));
		double accuracyTrainRF = evaluator.evaluate(predictionsTrainDataRF);
		System.out.println("Train Accuracy for Random Forest		= " + (accuracyTrainRF));
		
		//Evaluate Accuracy for Decision Tree
		double accuracyDT = evaluator.evaluate(predictionsDT);
		System.out.println("Test Accuracy for Decision Tree		= " + (accuracyDT));
		double accuracyTrainDT = evaluator.evaluate(predictionsTrainDT);
		System.out.println("Train Accuracy for Decision Tree		= " + (accuracyTrainDT));
		
		System.out.println();
		
		evaluator.setMetricName("f1");

		//Evaluate F1 for Random Forest
		double f1TestRF = evaluator.evaluate(predictionsTestDataRF);
		System.out.println("Test F1 for Random Forest				= " + (f1TestRF));
		double f1TrainRF = evaluator.evaluate(predictionsTrainDataRF);
		System.out.println("Train F1 for Random Forest			= " + (f1TrainRF));


		//Evaluate F1 for Decision Tree
		double f1TestDF = evaluator.evaluate(predictionsDT);
		System.out.println("Test F1 for Decision Forest			= " + (f1TestDF));
		double f1TrainDF = evaluator.evaluate(predictionsTrainDT);
		System.out.println("Train F1 for Decision Forest			= " + (f1TrainDF));

		System.out.println();
		
		evaluator.setMetricName("weightedPrecision");
		
		//Evaluate Precision for Random Forest
		double precisionTestRF = evaluator.evaluate(predictionsTestDataRF);
		System.out.println("Test precision for Random Forest		= " + (precisionTestRF));
		double precisionTrainRF = evaluator.evaluate(predictionsTrainDataRF);
		System.out.println("Train precision for Random Forest		= " + (precisionTrainRF));


		//Evaluate Precision for Decision Tree
		double precisionTestDF = evaluator.evaluate(predictionsDT);
		System.out.println("Test precision for Decision Forest	= " + (precisionTestDF));
		double precisionTrainDF = evaluator.evaluate(predictionsTrainDT);
		System.out.println("Train precision for Decision Forest	= " + (precisionTrainDF));
		
		System.out.println();
		
		evaluator.setMetricName("weightedRecall");
		
		//Evaluate Recall for Random Forest
		double recallTestRF = evaluator.evaluate(predictionsTestDataRF);
		System.out.println("Test recall for Random Forest			= " + (recallTestRF));
		double recallTrainRF = evaluator.evaluate(predictionsTrainDataRF);
		System.out.println("Train recall for Random Forest		= " + (recallTrainRF));


		//Evaluate Recall for Decision Tree
		double recallTestDF = evaluator.evaluate(predictionsDT);
		System.out.println("Test recall for Decision Forest		= " + (recallTestDF));
		//Evaluate Random Forest
		double recallTrainDF = evaluator.evaluate(predictionsTrainDT);
		System.out.println("Train recall for Decision Forest		= " + (recallTrainDF));
		
		System.out.println();
		
		// View confusion matrix
		System.out.println("Random Forest Confusion Matrix for Test Data :");
		predictionsTestDataRF.groupBy(col("gender"), col("predictedLabel")).count().show();

		System.out.println();
		
		// View confusion matrix
		System.out.println("Decision Tree Confusion Matrix for Test Data :");
		predictionsDT.groupBy(col("gender"), col("predictedLabel")).count().show();
		
		
	}

}
