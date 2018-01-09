package fr.Mohand;

import java.io.IOException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class Main {
	private static final Pattern SPACE = Pattern.compile("\\s+");

	@SuppressWarnings({ "serial" })
	public static void main(String[] args) throws InterruptedException, IOException {

		Logger.getRootLogger().setLevel(Level.WARN);
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Spark_Streaming_Application");
		@SuppressWarnings("resource")
		JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(10000));

		JavaInputDStream<String> lines = ssc.receiverStream(new JavaCustomReceiver());
		JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());
	    JavaDStream<String> hashTags = words.filter(new Function<String, Boolean>() {
	        @Override
	        public Boolean call(String word) {
	          return word.startsWith("#");
	        }
	      });

	    JavaPairDStream<String, Integer> hashTagCount = hashTags.mapToPair(
	    	      new PairFunction<String, String, Integer>() {
	    	        @Override
	    	        public Tuple2<String, Integer> call(String s) {
	    	          // leave out the # character
	    	          return new Tuple2<>(s.substring(1), 1);
	    	        }
	    	      });

	    	    JavaPairDStream<String, Integer> hashTagTotals = hashTagCount.reduceByKey((a, b) -> a + b);

	    hashTagTotals.print();
		ssc.start();
		ssc.awaitTermination();
	}
}
