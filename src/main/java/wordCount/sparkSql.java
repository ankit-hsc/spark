package wordCount;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;






public class sparkSql {

	public static void main(String[] args) throws AnalysisException {
		System.setProperty("hadoop.home.dir", "C:\\winutils\\");	
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		
		SparkSession spark = SparkSession
			      .builder()
			      .appName("Java Spark SQL")
			      .master("local")
			      .getOrCreate();
		 
		
	/*	SparkConf conf =new SparkConf();
		conf.setAppName("Demo");
		conf.setMaster("local");
		SparkContext sc=new SparkContext(conf);
		SQLContext sqlc=SQLContext.getOrCreate(sc);*/
		
		 
		 Dataset<Row> df =spark.read().json("new.json");
		 
		// ------------------Create DataFrame & view------------------
		 
		/* df.show();
		 df.printSchema();
		 df.select("age").show();
		 df.groupBy("name").count().show();
		 df.select(col("name"),col("age").plus(1)).show();
		 df.filter(col("name").eqNullSafe("Amit")).show();
		 df.filter(col("age").gt(21)).show();
	
		 Dataset<Person> personData=df.as(Encoders.bean(Person.class));
		 personData.show();
		 
		 df.createOrReplaceTempView("people");
		 Dataset<Row> sqlView=spark.sql("select * from people");
		 sqlView.show();
		 
		 df.createGlobalTempView("people");
		 Dataset<Row> sqlGlobal=spark.sql("select * from global_temp.people");
		 sqlGlobal.show();
		 spark.newSession().sql("select * from global_temp.people").show();*/
		 
		 
		 //--------------dataset From bean class-------------------
		 
		 
	/*	 Person person =new Person();
		 person.setAge(21);
		 person.setName("Ankit");
		 
		 Encoder<Person> personEncoder=Encoders.bean(Person.class);
		 Dataset<Person> personDf=spark.createDataset(Collections.singletonList(person), personEncoder);
		 personDf.show();
		 
		 
		 Encoder<Integer> intEncoder=Encoders.INT();
		 Dataset<Integer> intDF=spark.createDataset(Arrays.asList(1,2,3), intEncoder);
		 intDF.show();
		 
		 Dataset<Integer> transDF=intDF.map(val->val+1,intEncoder);
		 transDF.show();*/
		 
		 
		 
		 //-----------------------Infer Schema ---------------------------
		 
		 
		 JavaRDD<Person> personRdd=spark.read().textFile("person.txt").javaRDD().map(
         line->{
        	 String[] val=line.split(",");
        	 Person person =new Person();
        	 person.setName(val[0].trim());
        	 person.setAge(Integer.parseInt(val[1].trim()));
        	 return person;
         });
		 
		 Dataset<Row> personDF=spark.createDataFrame(personRdd, Person.class);
		 personDF.show();
		 
		 personDF.createOrReplaceTempView("people");
		 Dataset<Row> sqlDF=spark.sql("select name from people where age between 13 and 19");
		 sqlDF.show();
		 
		 //--------------
		 
		 
		 spark.stop();
	}

}
