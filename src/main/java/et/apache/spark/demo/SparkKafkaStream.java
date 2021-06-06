package et.apache.spark.demo;


import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.to_json;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
//@Component
public class SparkKafkaStream 
	//implements ApplicationRunner
{
	public static void main(String[] args) throws Exception {
		SparkKafkaStream sk = new SparkKafkaStream();
		sk.run(null);
	}
//	@Override
	public void run(ApplicationArguments args) throws Exception {
		
		SparkConf conf = new SparkConf().setAppName("test")
				.setMaster("local[1]");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		var ss = SparkSession.builder().getOrCreate();
		
		var df = ss.readStream()
					.format("kafka")
					.option("kafka.bootstrap.servers", "localhost:9092")
					.option("subscribe","json_topic")
					.option("startingOffsets","earliest")
					.load();
		//schema of streaming data from Kafka
		df.printSchema();
		//convert the binary value to String 
		var personStringDf = df.selectExpr("CAST(value AS STRING)");
		
		//custom schema
		var schema = new StructType()
				.add(new StructField("id",DataTypes.IntegerType,true , new Metadata()))
				.add(new StructField("firstname",DataTypes.StringType,true , new Metadata()))
				.add(new StructField("middlename",DataTypes.StringType,true , new Metadata()))
				.add(new StructField("lastname",DataTypes.StringType,true , new Metadata()))
				.add(new StructField("dob_year",DataTypes.IntegerType,true , new Metadata()))
				.add(new StructField("dob_month",DataTypes.IntegerType,true , new Metadata()))
				.add(new StructField("gender",DataTypes.StringType,true , new Metadata()))
				.add(new StructField("salary",DataTypes.IntegerType,true , new Metadata()))
				;
		
		//extract the value to DataFrame using custom schema
		var personDF = personStringDf
				.select(from_json(col("value"),schema)
						.as("data"))
				.select("data.*");
		
		// print to console
		
//		personDF.writeStream()
//	      .format("console")
//	      .outputMode("append")
//	      .start()
//	      .awaitTermination();
//		
		//write Spark Streaming data to Kafka
		//value column is required and all other fields are optional. 
		//first convert binary key,values to string
		var pDf = personDF.selectExpr("CAST(id AS STRING) AS key");
		//Since we are processing JSON, 
		//let’s convert data to JSON using to_json() 
		//function and store it in a value column.
		var toKafka = pDf.select(to_json(struct("*")).as("value"));
//				;
		toKafka.writeStream()
			.format("kafka")
			.outputMode("append")// writing as-is, without aggregation
			.option("kafka.bootstrap.servers","localhost:9092")
			.option("checkpointLocation", "/tmp/checkpoint")
			.option("topic","json_data_topic")
			.start()
			.awaitTermination();
	
//		df.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value")
//		   .writeStream()
//		   .format("kafka")
//		   .outputMode("append")
//		   .option("kafka.bootstrap.servers", "localhost:9092")
//		   .option("topic", "josn_data_topic")
//		   .start()
//		   .awaitTermination();
	}

}
