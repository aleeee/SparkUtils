package et.apache.spark.demo;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import et.apache.spark.sentiment.SentimentUtils;
import scala.Tuple2;
//@Component
public class KafkaTweetsConsumer3  implements ApplicationRunner, Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static final Logger logger = LoggerFactory.getLogger(KafkaTweetsConsumer3.class);
	
	@Value("${bootstrap.servers}")
	String brokerList;
	@Value("${destination.topics}")
	String destinationTopics;
	
	@Value("${elastic.index.name}")
	String tweetsIndexName;
	
	@Value("${elastic.url}")
	String esUrl;
	
	@Value("${elastic.port}")
	String esPort;

	static final int BATCH_DURATION= 5000;
	static final String GROUP_ID ="twitter-group1";
	 private static final String NODES = "spark.es.nodes";
	    private static final String PORT = "spark.es.port";
	    private static final String RESOURCE = "spark.es.resource";
	    private static final String INDEX_AUTO_CREATE = "spark.es.index.auto.create";
	ObjectMapper mapper = new ObjectMapper();
	@Override
	public void run(ApplicationArguments args) throws Exception {
		SparkConf conf = new SparkConf()
				.setMaster("local[*]")
				.setAppName("test");
		
		JavaStreamingContext  sc = new JavaStreamingContext(conf,new Duration(BATCH_DURATION));
		
		Map<String,Object> kafkaParams = new HashMap<>();
//		kafkaParams.put("bootstrap.servers",BROKER_LIST);
		 kafkaParams.put("metadata.broker.list", brokerList);
		    kafkaParams.put("bootstrap.servers", brokerList);
//		kafkaParams.put("zookeeper.connect" ,"localhost:2181");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id",GROUP_ID);
		kafkaParams.put("auto.offset.reset", "earliest");
		//kafkaParams.put("max.poll.records", 10);
		kafkaParams.put("enable.auto.commit", false);
		
		Collection<String> topics = Arrays.asList(destinationTopics);
		
	 
		JavaInputDStream<ConsumerRecord<String, String>> tweets =
				KafkaUtils.
					createDirectStream(
							sc,
							LocationStrategies.PreferConsistent(),
							ConsumerStrategies.<String,String>
								Subscribe(topics, kafkaParams));
//		JavaSparkContext jsc = new JavaSparkContext(conf);                              
//		JavaStreamingContext jssc = new JavaStreamingContext(jsc, Seconds.apply(1));
		streamToEs(tweets,sc);
		
		 sc.start();
		sc.awaitTermination();
		
//var ss = SparkSession.builder().getOrCreate();
//		
//		var df = ss.readStream()
//					.format("kafka")
//					.option("kafka.bootstrap.servers", "localhost:9092")
//					.option("subscribe","json_topic")
//					.option("startingOffsets","earliest")
//					.load();
//		//schema of streaming data from Kafka
//		df.printSchema();
		
	}
	private void streamToEs(JavaInputDStream<ConsumerRecord<String, String>> tweets, JavaStreamingContext jssc)
			throws InterruptedException, IOException {
		JavaDStream<String> tStream = tweets.flatMap(new FlatMapFunction<ConsumerRecord<String, String>, String>() {
		      @Override
		      public Iterator<String> call(ConsumerRecord<String, String> s) throws JsonProcessingException {
		    	  Tweet t = mapper.readValue(s.value(),Tweet.class);
		        List<String> list = Arrays.asList(mapper.writeValueAsString(s.value()));
		        System.out.println("tweet" + t);
//		        JavaDStream<Map<String, ?>> javaDStream = jssc2.para(list);
//		        JavaEsSparkStreaming.saveToEs(jssc.parallelize(list), "spark/docs");                       
//		        JavaEsSpark.saveJsonToEs(jssc.parallelize(list),"tweets");  
		        return list.iterator();
		      }
		    });
		JavaDStream<String> words = tStream.flatMap(new FlatMapFunction<String, String>() {
		      @Override
		      public Iterator<String> call(String s) {
		        return Arrays.asList(s.split(" ")).iterator();
		      }
		    });
		hashTagAnalysis(words);
		
	
	}
	
	 public static void hashTagAnalysis(JavaDStream<String> tStream ) {
		 
	        
	        JavaPairDStream<String, Double> tweetWithScoreDStream =
	                tStream.mapToPair(tweetText -> new Tuple2<>(tweetText, Double.valueOf(SentimentUtils.calculateWeightedSentimentScore(tweetText))));

	        tweetWithScoreDStream.print();
	    }
}
