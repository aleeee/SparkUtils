//package et.apache.spark.cfg;
//
//import java.text.SimpleDateFormat;
//import org.apache.http.HttpHost;
//import org.elasticsearch.client.RestClient;
//import org.elasticsearch.client.RestHighLevelClient;
//import org.springframework.beans.factory.annotation.Value;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//import org.springframework.data.elasticsearch.config.AbstractElasticsearchConfiguration;
//@Configuration
//public class ElasticSearchConfig extends AbstractElasticsearchConfiguration{
//	@Value("${elastic.url}")
//	String elasticSearchUrl;
//	@Value("${elastic.port}")
//	int elasticSearchPort;
//	 private static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
//
//	@Bean
//	@Override
//	public RestHighLevelClient elasticsearchClient() {
//		RestHighLevelClient client = new RestHighLevelClient(
//				RestClient.builder(new HttpHost(elasticSearchUrl, elasticSearchPort, "http")));
//		
//		return client;
//	}
//
//		
//
//}
