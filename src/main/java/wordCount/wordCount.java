package wordCount;



import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class wordCount {

	public static void main(String[] args) {
	
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		System.setProperty("hadoop.home.dir", "C:\\winutils\\");
		
		
		
		SparkConf conf =new SparkConf();
		conf.setAppName("Demo");
		conf.setMaster("local");
		JavaSparkContext sc=new JavaSparkContext(conf);
		
		
		sc.setLogLevel("WARN");
		JavaRDD<String> lines=sc.textFile("E:\\git.txt");
		System.out.println(lines.count());
		System.out.println("more than 5 words: "+lines.filter(line->line.split(" ").length>5).collect().toString());
		System.out.println("contains git: "+lines.filter(line->line.contains("git")).count());
		System.out.println("contains git: "+lines.take(2));
	}

}
