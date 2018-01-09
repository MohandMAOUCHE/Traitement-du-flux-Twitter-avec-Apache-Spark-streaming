package fr.Mohand;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class MainLetterCount {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		final String APP_NAME = "test";
		String master = "local[*]";
		SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster(master);
		JavaSparkContext sc = new JavaSparkContext(conf);
		LetterCount.createJob(sc, master);
	}

}
