package wordCount;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;

import scala.Tuple2;

public class sparkRdd {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:\\winutils\\");
		SparkConf conf =new SparkConf();
		conf.setAppName("rdd");
		conf.setMaster("local");
		
		JavaSparkContext sc =new JavaSparkContext(conf);
		JavaRDD<Integer> numberRdd=sc.parallelize(Arrays.asList(1,2,3,4));
		System.out.println(numberRdd.take(2));
		System.out.println(numberRdd.reduce((n1,n2)->n1+n2));
		
		JavaRDD<Integer> squareRdd=numberRdd.map(n->n*n);
		System.out.println(squareRdd.collect().toString());
		
		System.out.println(numberRdd.union(squareRdd).count());
		
		System.out.println(numberRdd.reduce((n1,n2)->n1+n2));

		
		JavaRDD<Integer> evenRdd=squareRdd.filter(n->n%2==0);
		System.out.println(evenRdd.collect().toString());
		
		JavaRDD<Integer> multiplyRdd=numberRdd.flatMap(n->Arrays.asList(n,n*2,n*3,n*4).iterator());
		System.out.println(multiplyRdd.collect().toString());
		
		JavaPairRDD<String,Integer> petRdd=JavaPairRDD.fromJavaRDD(sc.parallelize(Arrays.asList(new Tuple2<String,Integer>("LION",1),
				new Tuple2<String,Integer>("CAT",2),new Tuple2<String,Integer>("DOG",3))));
		System.out.println(petRdd.collect().toString());
		
		
		JavaPairRDD<String,Integer> MypetRdd=JavaPairRDD.fromJavaRDD(sc.parallelize(Arrays.asList(
				new Tuple2<String,Integer>("CAT",6),new Tuple2<String,Integer>("DOG",7),new Tuple2<String,Integer>("MAN",8))));
		System.out.println(MypetRdd.collect().toString());
        
		JavaPairRDD<String,Integer> agedPetsRDD = petRdd.reduceByKey((v1,v2)->Math.max(v1, v2));
		System.out.println(agedPetsRDD.collect().toString());
		
		JavaPairRDD<String, Iterable<Integer>> groupRdd=petRdd.groupByKey();
		 System.out.println(groupRdd.collect().toString());
		 
		 JavaPairRDD<String, Tuple2<Optional<Integer>,Integer>> joinRdd=petRdd.rightOuterJoin(MypetRdd);
		 System.out.println(joinRdd.collect().toString());
		 
		 JavaPairRDD<String, Tuple2<Iterable<Integer>,Iterable<Integer>>> coGroupRdd=petRdd.cogroup(MypetRdd);
		 System.out.println(coGroupRdd.collect().toString());
	}

}
