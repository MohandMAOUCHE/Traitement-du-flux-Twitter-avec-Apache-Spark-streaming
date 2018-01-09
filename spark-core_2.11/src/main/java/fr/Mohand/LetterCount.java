package fr.Mohand;

import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class LetterCount {
    private final static char naiveTolower = (char) ('A'-'a');

	public static JavaPairRDD<Character, Integer> createJob(JavaSparkContext sc, String inFile) {

		JavaRDD<String> textFile = sc.textFile("C:\\Users\\Cartman\\Desktop\\texte.txt");
		JavaRDD<String> words = textFile.flatMap(line -> Arrays.stream(line.trim().split(" ")).iterator());
		JavaRDD<Character> letters = words.flatMap(word -> word.chars().mapToObj(c -> (char)c).iterator());
		JavaRDD<Character> lettersLowered = letters.map(c -> c.charValue() >= 'A' && c.charValue() <= 'Z' ? (char)(c.charValue() - naiveTolower) : c);
		JavaRDD<Character> lettersLoweredFiltered = lettersLowered.filter(i -> i >= 'a' && i <= 'z');
		JavaPairRDD<Character, Integer> pairs = lettersLoweredFiltered.mapToPair(c -> new Tuple2<Character, Integer>(c, 1));
		JavaPairRDD<Character, Integer> counts = pairs.reduceByKey((a, b) -> a + b);
		System.out.println("Counts = "+counts.countByValue());
		return counts;
	}
}