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
public class SparkStreamingFilesFromFolder implements ApplicationRunner {

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
				.add(new StructField("RecordNumber", DataTypes.IntegerType, true,  Metadata.empty()))
				.add(new StructField("Zipcode", DataTypes.StringType, true, Metadata.empty()))
				.add(new StructField("ZipcodeType", DataTypes.StringType, true, Metadata.empty()))
				.add(new StructField("City", DataTypes.StringType, true, Metadata.empty()))
				.add(new StructField("State", DataTypes.StringType, true, Metadata.empty()))
				.add(new StructField("LocationType", DataTypes.StringType, true, Metadata.empty()))
				.add(new StructField("Lat", DataTypes.StringType, true, Metadata.empty()))
				.add(new StructField("Long", DataTypes.StringType, true, Metadata.empty()))
				.add(new StructField("Xaxis", DataTypes.StringType, true, Metadata.empty()))
				.add(new StructField("Yaxis", DataTypes.StringType, true, Metadata.empty()))
				.add(new StructField("Zaxis", DataTypes.StringType, true, Metadata.empty()))
				.add(new StructField("WorldRegion", DataTypes.StringType, true, Metadata.empty()))
				.add(new StructField("Country", DataTypes.StringType, true, Metadata.empty()))
				.add(new StructField("LocationText", DataTypes.StringType, true, Metadata.empty()))
				.add(new StructField("Location", DataTypes.StringType, true, Metadata.empty()))
				.add(new StructField("Decommisioned", DataTypes.StringType, true, Metadata.empty()));
		
		//load data 
		var df = ss.readStream()
				.schema(schema)
				.json("/home/adwa/Spark/inputs/zipcode");
		
		df.printSchema();
		
		var groupDf= df.select("Zipcode")
				.groupBy("Zipcode")
				.count();
		groupDf.printSchema();
		
		groupDf.writeStream()
			.format("console")
			.outputMode("complete")// aggregated
			.start()
			.awaitTermination();
	}

}
