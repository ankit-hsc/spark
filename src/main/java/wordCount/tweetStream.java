package wordCount;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.twitter.*;

import twitter4j.Status;

public class tweetStream {

	public static void main(String[] args) {
		
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		System.setProperty("hadoop.home.dir", "C:\\winutils\\");
		
		

		 final String consumerKey = "HAqyV1z3nXXuzqFuwuuZliKxJ";
	        final String consumerSecret = "dAFXccYjGp0CGskt7ws56El88yfPEtjELWsOKijiUYFD9DFdcY";
	        final String accessToken = "746502584525950979-p4XqG571f2iM1q546pAMOIY6OTxB56H";
	        final String accessTokenSecret = "LTG2YLI9iSNkAT6TbPXyuDqjzLPGVqu2VncrTaigpFgBH";

	        SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkTwitterHelloWorldExample");
	        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(1000));

	        System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
	        System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
	        System.setProperty("twitter4j.oauth.accessToken", accessToken);
	        System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);

	        JavaReceiverInputDStream<Status> twitterStream = TwitterUtils.createStream(jssc);

	        // Without filter: Output text of all tweets
	        JavaDStream<String> statuses = twitterStream.map(
	                new Function<Status, String>() {
	                    public String call(Status status) { return status.getText(); }
	                }
	        );
	
	        // With filter: Only use tweets with geolocation and print location+text.
	        /*JavaDStream<Status> tweetsWithLocation = twitterStream.filter(
	                new Function<Status, Boolean>() {
	                    public Boolean call(Status status){
	                        if (status.getGeoLocation() != null) {
	                            return true;
	                        } else {
	                            return false;
	                        }
	                    }
	                }
	        );
	        JavaDStream<String> statuses = tweetsWithLocation.map(
	                new Function<Status, String>() {
	                    public String call(Status status) {
	                        return status.getGeoLocation().toString() + ": " + status.getText();
	                    }
	                }
	        );*/

	        statuses.print();
	        jssc.start();
	        jssc.awaitTerminationOrTimeout(5000);
	
	}

}
