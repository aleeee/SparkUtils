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
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Seconds;
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

import scala.Tuple2;
//@Component
public class KafkaTweetsConsumer2  implements ApplicationRunner, Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static final Logger logger = LoggerFactory.getLogger(KafkaTweetsConsumer2.class);
	
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
		conf.set(NODES, esUrl);
	        conf.set(PORT, esPort);
	        conf.set(RESOURCE, tweetsIndexName);
	        conf.set(INDEX_AUTO_CREATE, "false");
	        conf.set("es.batch.size.entries", "0");
	        conf.set("es.batch.size.bytes", "0");
	        conf.set("es.batch.concurrent.request","1");
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
		kafkaParams.put("max.poll.records", 10);
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
		 
		JavaDStream<String> hashTags = words.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String word) {
				return word.startsWith("#");
			}
		});

		// Read in the word-sentiment list and create a static RDD from it
		String wordSentimentFilePath = "src/main/resources/AFINN-111.txt";
		final JavaPairRDD<String, Double> wordSentiments = jssc.sparkContext().textFile(wordSentimentFilePath)
				.mapToPair(new PairFunction<String, String, Double>() {
					@Override
					public Tuple2<String, Double> call(String line) {
						String[] columns = line.split("\t");
						return new Tuple2<>(columns[0], Double.parseDouble(columns[1]));
					}
				});

		JavaPairDStream<String, Integer> hashTagCount = hashTags.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String s) {
				// leave out the # character
				logger.info("hashTagCount {}",s);
				return new Tuple2<>(s.substring(1), 1);
			}
		});

		JavaPairDStream<String, Integer> hashTagTotals = hashTagCount
				.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
					@Override
					public Integer call(Integer a, Integer b) {
						return a + b;
					}
				}, new Duration(10000));
		logger.info("hashTagTotals {}",hashTagTotals);
		// Determine the hash tags with the highest sentiment values by joining the
		// streaming RDD
		// with the static RDD inside the transform() method and then multiplying
		// the frequency of the hash tag by its sentiment value
		JavaPairDStream<String, Tuple2<Double, Integer>> joinedTuples = hashTagTotals.transformToPair(
				new Function<JavaPairRDD<String, Integer>, JavaPairRDD<String, Tuple2<Double, Integer>>>() {
					@Override
					public JavaPairRDD<String, Tuple2<Double, Integer>> call(JavaPairRDD<String, Integer> topicCount) {
						return wordSentiments.join(topicCount);
					}
				});

		JavaPairDStream<String, Double> topicHappiness = joinedTuples
				.mapToPair(new PairFunction<Tuple2<String, Tuple2<Double, Integer>>, String, Double>() {
					@Override
					public Tuple2<String, Double> call(Tuple2<String, Tuple2<Double, Integer>> topicAndTuplePair) {
						Tuple2<Double, Integer> happinessAndCount = topicAndTuplePair._2();
						logger.info("happinessAndCount {}",happinessAndCount);
						return new Tuple2<>(topicAndTuplePair._1(), happinessAndCount._1() * happinessAndCount._2());
					}
				});

		JavaPairDStream<Double, String> happinessTopicPairs = topicHappiness
				.mapToPair(new PairFunction<Tuple2<String, Double>, Double, String>() {
					@Override
					public Tuple2<Double, String> call(Tuple2<String, Double> topicHappiness) {
						return new Tuple2<>(topicHappiness._2(), topicHappiness._1());
					}
				});

		JavaPairDStream<Double, String> happiest10 = happinessTopicPairs
				.transformToPair(new Function<JavaPairRDD<Double, String>, JavaPairRDD<Double, String>>() {
					@Override
					public JavaPairRDD<Double, String> call(JavaPairRDD<Double, String> happinessAndTopics) {
						return happinessAndTopics.sortByKey(false);
					}
				});

		// Print hash tags with the most positive sentiment values
		happiest10.foreachRDD(new VoidFunction<JavaPairRDD<Double, String>>() {
			@Override
			public void call(JavaPairRDD<Double, String> happinessTopicPairs) {
				List<Tuple2<Double, String>> topList = happinessTopicPairs.take(10);
				logger.info("topList {}",topList);
				System.out.println(
						String.format("\nHappiest topics in last 10 seconds (%s total):", happinessTopicPairs.count()));
				for (Tuple2<Double, String> pair : topList) {
					System.out.println(String.format("%s (%s happiness)", pair._2(), pair._1()));
				}
			}
		});
	
	}
	
	
}
