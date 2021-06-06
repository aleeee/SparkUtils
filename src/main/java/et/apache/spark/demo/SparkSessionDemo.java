package et.apache.spark.demo;

import org.apache.spark.sql.SparkSession;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;
//@Component
public class SparkSessionDemo implements ApplicationRunner{
	
	@Override
	public void run(ApplicationArguments args) throws Exception {
		var sprkSession =  SparkSession.builder()
				.master("local[1]")
				.appName("sparkDemo")
				.getOrCreate();
		
		System.out.println("AppName " + sprkSession.sparkContext().appName());
		System.out.println("DeployMode " + sprkSession.sparkContext().deployMode());
		System.out.println("Master " + sprkSession.sparkContext().master());
		
		var sprkSession2 =  SparkSession.builder()
				.master("local[1]")
				.appName("sparkDemo-2")
				.getOrCreate();
		
		System.out.println("App2Name " + sprkSession2.sparkContext().appName());
		System.out.println("DeployMode2 " + sprkSession2.sparkContext().deployMode());
		System.out.println("Master2 " + sprkSession2.sparkContext().master());
		
	
	}

}
