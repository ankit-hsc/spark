package wordCount;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class sparkStreaming {

	public static void main(String[] args) throws InterruptedException {

		
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		System.setProperty("hadoop.home.dir", "C:\\winutils\\");
		
		SparkConf conf =new SparkConf();
		conf.setAppName("Demo");
		conf.setMaster("local");
		JavaSparkContext sc=new JavaSparkContext(conf);
		
		JavaStreamingContext stream=new JavaStreamingContext(sc, new Duration(1000));
		JavaDStream<String> lines=stream.socketTextStream("localhost", 7777);
		
		JavaDStream<String> result=lines.map(line->line.toString());
		
		result.print();
		
		stream.start();
     	stream.awaitTerminationOrTimeout(5000);
		
	}

}
