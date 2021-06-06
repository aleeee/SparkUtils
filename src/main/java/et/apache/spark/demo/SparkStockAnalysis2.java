package et.apache.spark.demo;

import static org.apache.spark.sql.functions.column;
import static org.apache.spark.sql.functions.desc;
import static org.apache.spark.sql.functions.year;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;
import org.apache.spark.mllib.stat.Statistics;
//@Component
public class SparkStockAnalysis2 implements ApplicationRunner {

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
		var btc = ss.read()
				.schema(schema)
				.option("header","true")
				.csv("/home/adwa/Spark/inputs/stock/btc5y.csv");
		
		var eth = ss.read()
				.schema(schema)
				.option("header","true")
				.csv("/home/adwa/Spark/inputs/stock/eth5y.csv");
		
		var ada = ss.read()
				.schema(schema)
				.option("header","true")
				.csv("/home/adwa/Spark/inputs/stock/ada5y.csv");
		
		var gold = ss.read()
				.schema(schema)
				.option("header","true")
				.csv("/home/adwa/Spark/inputs/stock/gold5y.csv");
		
		//compute the average closing price per year
		
//		btc.selectExpr("year(Date) as Date","`Adj Close`") 
//			.groupBy("Date")
//			.avg("`Adj Close`")
//			.orderBy(desc("Date"))
//			.show();
//		
//		eth.selectExpr("year(Date) as Date","`Adj Close`") 
//		.groupBy("Date")
//		.avg("`Adj Close`")
//		.orderBy(desc("Date"))
//		.show();
//		ada.selectExpr("year(Date) as Date","`Adj Close`") 
//		.groupBy("Date")
//		.avg("`Adj Close`")
//		.orderBy(desc("Date"))
//		.show();
//		gold.selectExpr("year(Date) as Date","`Adj Close`") 
//		.groupBy("Date")
//		.avg("`Adj Close`")
//		.orderBy(desc("Date"))
//		.show();
		
		// average closing price per month
		ada.createOrReplaceTempView("ada");
		btc.createOrReplaceTempView("btc");
		eth.createOrReplaceTempView("eth");
		gold.createOrReplaceTempView("gold");
		
//		ada.sqlContext().sql("select year(Date) as yr,month(Date) as mo, avg(`Adj Close`) as avgPrice"
//				+ " from ada group By yr,mo order by yr desc, mo desc ")
//		.show();
		
//		show top opening and closing price differences
//		ss.sqlContext().sql("select Date, Open, Close, abs( Close - Open) as priceDifference from ada "
//				+ " order by abs(Close - Open) desc")
//			.show();
		
		//join and compare
		var joinedStocks = ss.sqlContext().sql("select ada.Date, ada.`Adj Close` as adaAdjClose, btc.`Adj Close` as btcAdjClose, "
				+ " eth.`Adj Close` as ethAdjClose, gold.`Adj Close` as goldAdjClose "
				+ " from ada join  btc on ada.Date = btc.Date join eth on eth.Date = ada.Date join gold on ada.Date = gold.Date "
				+ " order by Date desc")
				.cache();
		joinedStocks.show();
		joinedStocks.createOrReplaceTempView("joinedStock");
		
		//calculate the averages by year
//		ss.sqlContext().sql("select year(Date), avg(adaAdjClose), avg(btcAdjClose), avg(ethAdjClose), avg(goldAdjClose) "
//				 +" from joinedStock group by year(Date) order by year(Date) desc")
//		.show();
		
		
		// save the view as a parquet
		joinedStocks.write().format("parquet").save("/home/adwa/Spark/inputs/stock/tmp/stocks.parquet");
		//calculate the correlation between them
		var stockDf = ss.sqlContext().read().parquet("/home/adwa/Spark/inputs/stock/tmp/stocks.parquet");
		stockDf.show();
		var closingAda = stockDf.select("adaAdjClose")
				.map((MapFunction<Row,Double>)row -> row.getAs("adaAdjClose"), Encoders.DOUBLE())
				.javaRDD();
		var closingBtc = stockDf.select("btcAdjClose")
				.map((MapFunction<Row,Double>)row -> row.getAs("btcAdjClose"), Encoders.DOUBLE())
				.javaRDD();
		var closingEth = stockDf.select("ethAdjClose").map((MapFunction<Row,Double>)row -> row.getAs("ethAdjClose"), Encoders.DOUBLE())
				.javaRDD();
		var closingGold = stockDf.select("goldAdjClose").map((MapFunction<Row,Double>)row -> row.getAs("goldAdjClose"), Encoders.DOUBLE())
				.javaRDD();
		
		var correlation = Statistics.corr(closingAda,closingBtc);
		
		System.out.println("correlation : "+ correlation);
	}

}
