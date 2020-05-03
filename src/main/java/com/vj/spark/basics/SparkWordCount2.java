package com.vj.spark.basics;

import java.util.Arrays;
import java.util.Scanner;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;
/**
 * 
 * @author rami
 * 
 * Using SparkSession.builder()
 */
public class SparkWordCount2 {

	public static void main(String[] args) {
		
		//System.getProperty("hadoop.home.dir" ,"c:/hadoop");
		//Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		
	    SparkSession spark = SparkSession.builder()
	    								.master("local[*]")
	    								.appName("Spark Word Count2").getOrCreate();
	    
		Dataset<String> textFileDataset = spark.read().textFile("src/main/resources/input/input.txt");
	    
		
		//System.out.println(textFileRDD.count());
		
		JavaPairRDD<String, Long> outputRdd = textFileDataset.toJavaRDD().flatMap( x->  Arrays.asList(x.split(" ")).iterator())
														   .map(x-> x.replaceAll("[^a-zA-Z]", ""))
														   .filter(x-> x.length() > 5)
														   .mapToPair(x-> new Tuple2<String,Long>(x,1L))
														   .reduceByKey((a,b)->(a+b));
												
		outputRdd.saveAsTextFile("src/main/resources/output");
		
		// included scanner below to view DAG on 
		//20/05/02 22:39:27 INFO SparkUI: Bound SparkUI to 0.0.0.0, and started at http://localhost:4040
		@SuppressWarnings("resource")
		Scanner scanner = new Scanner(System.in);
		scanner.nextLine();
		
		spark.close();
	}

}

//mvn exec:java -Dexec.mainClass=com.vj.spark.basics.SparkWordCount 
//mvn exec:java -Dexec.mainClass=com.vj.spark.basics.SparkWordCount -Dexec.args="src/main/resources/input/input.txt"

