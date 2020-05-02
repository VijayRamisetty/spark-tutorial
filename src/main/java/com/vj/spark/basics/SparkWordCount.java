package com.vj.spark.basics;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkWordCount {

	public static void main(String[] args) {
		
		//System.getProperty("hadoop.home.dir" ,"c:/hadoop");
		//Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		

		SparkConf conf = new SparkConf().setMaster("local[*]")
										.setAppName("Spark Word Count");
		JavaSparkContext  sc = new JavaSparkContext(conf);
		
		JavaRDD<String> textFileRDD = sc.textFile("src/main/resources/input/input.txt");
		
		//System.out.println(textFileRDD.count());
		
		JavaPairRDD<String, Long> outputRdd = textFileRDD.flatMap( x->  Arrays.asList(x.split(" ")).iterator())
														   .map(x-> x.replaceAll("[^a-zA-Z]", ""))
														   .filter(x-> x.length() > 5)
														   .mapToPair(x-> new Tuple2<String,Long>(x,1L))
														   .reduceByKey((a,b)->(a+b));
												
		outputRdd.saveAsTextFile("src/main/resources/output");
		sc.close();
	}

}

//mvn exec:java -Dexec.mainClass=com.vj.spark.basics.SparkWordCount 
//mvn exec:java -Dexec.mainClass=com.vj.spark.basics.SparkWordCount -Dexec.args="src/main/resources/input/input.txt"

