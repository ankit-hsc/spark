package wordCount;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.StringDecoder;

public class KafkaStream {

	public static void main(String[] args) {
		
		
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		System.setProperty("hadoop.home.dir", "C:\\winutils\\");
		
		
		
		   SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkKafka");
		   JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(2000));
		   
		   Map<String ,String> kafkaParams=new HashMap<>();
		   kafkaParams.put("metadata.broker.list", "localhost:9092");
		   Set<String> topics=Collections.singleton("demo-topic");
		   JavaPairInputDStream<String,String> directKafka=KafkaUtils.createDirectStream(jssc,String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);
		 
		   
		   //KafkaUtils.createStream(arg0, arg1, arg2, arg3);
		   
		   directKafka.foreachRDD(rdd->{
			   rdd.foreach(record->System.out.println(record._2));
		   });
		   
		   
		   jssc.start();
		   jssc.awaitTermination();
		   
	}

}
