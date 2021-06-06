package et.apache.spark.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

//@Component
public class SparkStockAnalysis implements ApplicationRunner {

	@Override
	public void run(ApplicationArguments args) throws Exception {
		SparkConf conf = new SparkConf().setAppName("test")
				.setMaster("local[1]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		var ss = SparkSession.builder()
				.getOrCreate();
		
		//create the schema
		//use Metadata.empty() to avoid null pointer exception 
		var schema = new StructType()
				.add(new StructField("Date", DataTypes.StringType, true, Metadata.empty()))
				.add(new StructField("Open", DataTypes.DoubleType, true, Metadata.empty()))
				.add(new StructField("High", DataTypes.DoubleType, true, Metadata.empty()))
				.add(new StructField("Low", DataTypes.DoubleType, true, Metadata.empty()))
				.add(new StructField("Close", DataTypes.DoubleType, true, Metadata.empty()))
				.add(new StructField("Adj Close", DataTypes.DoubleType, true, Metadata.empty()))
				.add(new StructField("Volume", DataTypes.DoubleType, true, Metadata.empty()));
		
		//load data 
		var df = ss.readStream()
				.schema(schema)
				.option("header","true")
				.csv("/home/adwa/Spark/inputs/stock");
		
		df.printSchema();
//		df.show();
		var output = df.writeStream()
		.format("console")
		.outputMode("append")
		.start();
		
		output.processAllAvailable();
//		var groupDf= df.filter(row -> row.)("Zipcode")
//				.groupBy("Zipcode")
//				.count();
//		groupDf.printSchema();
//		
//		groupDf.writeStream()
//			.format("console")
//			.outputMode("complete")// aggregated
//			.start()
//			.awaitTermination();
	}

}
