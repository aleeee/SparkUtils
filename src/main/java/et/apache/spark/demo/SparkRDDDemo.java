package et.apache.spark.demo;


import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import scala.Tuple2;
//@Component
public class SparkRDDDemo implements ApplicationRunner{
	
	@Override
	public void run(ApplicationArguments args) throws Exception {
		
		SparkConf conf = new SparkConf().setAppName("test")
				.setMaster("local[1]");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		
		var seqData = Arrays.asList(new Tuple2<>("java",10), new Tuple2<>("python", 20), new Tuple2<>("scala", 30));
		
		var rdd = sc.parallelize(seqData);
		
		System.out.println(rdd.count());
		
		URL f = getClass().getResource("/input/data.txt");
		
		JavaRDD<String> lines = sc.textFile(f.getFile());
		
		JavaRDD<Integer> lineLengths = lines.map(s -> s.length());
		
		int totalLength = lineLengths.reduce((a,b) -> a+b);
		
		System.out.println("totalLength " + totalLength);
		
		lineLengths.persist(StorageLevel.MEMORY_ONLY());
		
	
	}

}
