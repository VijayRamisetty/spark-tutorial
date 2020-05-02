package com.vj.spark.basics;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * 
 * @author rami
 * 
 * More on Spark 
 * 
 * - Sort 
 * - Coalesce
 * - Take
 * - Collect
 */
public class SparkInternals {

	public static void main(String[] args) {
		
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkConf conf = new SparkConf().setMaster("local[*]")
										.setAppName("Spark Word Count");
		JavaSparkContext  sc = new JavaSparkContext(conf);

		
		
		JavaRDD<String> textFileRDD = sc.textFile("src/main/resources/input-new/*.txt");

		//word_count
		JavaPairRDD<String, Long> word_count = textFileRDD.flatMap( x->  Arrays.asList(x.split(" ")).iterator())
				.map(x-> x.replaceAll("[^a-zA-Z]", ""))
				.filter(x-> x.length() > 4)
				.mapToPair(x-> new Tuple2<String,Long>(x,1L))
				.reduceByKey((a,b)->(a+b));

		
		// count_word
		JavaPairRDD<Long, String> count_word = word_count.mapToPair(x -> new Tuple2<Long,String>(x._2,x._1));
	
		// descending
		JavaPairRDD<Long, String> sorted_count_word = count_word.sortByKey(false);
		
		// num partitions
		
		System.out.println("No of partitions "+sorted_count_word.getNumPartitions());
		
		// sort not works well with foreach 
		// reason : driver sending foreach print to all nodes and 
		// all nodes are running foreach println in parallel 
		// almost like multiple threads on single node trying to print ,
		// almost like multiple tasks on multiple nodes trying to print ,
		// i.e, foreach is executing the lambda on each partition in parallel
		sorted_count_word.foreach(x-> System.out.println(x));
		
		
		
		System.out.println("-------------------------------------------");
		// coalesce - in real this is bad idea to use coalesce
		// entire data as one partition blots up single node
		sorted_count_word.coalesce(1).foreach(x-> System.out.println(x));
		
		
		System.out.println("-------------------------------------------");
		sorted_count_word.take(1000)
						 .forEach(x-> System.out.println(x));  // <- java forEach not spark foreach
		
		
		//  collect 
		
		// sorted_count_word.collect()  <- return an array that contains all of the elements in this RDD.
		
		sc.close();
	}

	
}
