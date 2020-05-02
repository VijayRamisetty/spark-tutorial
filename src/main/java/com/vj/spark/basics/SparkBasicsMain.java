package com.vj.spark.basics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.collect.Iterables;

import scala.Tuple2;
/*
 *  
 *  
 */
public class SparkBasicsMain {


	public static void main(String[] args) {

		// handling logs
		Logger.getLogger("org.apache").setLevel(Level.WARN);;

		// conf & sc 
		SparkConf conf = new SparkConf()
				.setAppName("App_JavaCollectionToRdd")
				.setMaster("local[*]");

		JavaSparkContext sc = new JavaSparkContext(conf);

		// Java collection to RDD
		List<Double> inputData = new  ArrayList<Double>();
		inputData.add(35.5);
		inputData.add(12.49943);
		inputData.add(90.32);
		inputData.add(20.32);

		JavaRDD<Double> baseRdd = sc.parallelize(inputData); 

		// Reduce

		//baseRdd.reduce((Double val1, Double val2) -> val1+val2);
		Double result = baseRdd.reduce(( val1, val2) -> val1+val2);

		System.out.println(result);

		// Map
		JavaRDD<Double> sqrtRdd = baseRdd.map( value -> Math.sqrt(value));

		// foreach
		sqrtRdd.collect().forEach( System.out::println);   //  using forEach of java.lang
		sqrtRdd.foreach(value -> System.out.println(value)); 

		//count
		System.out.println(sqrtRdd.count());

		// get count using map and reduce

		JavaRDD<Long> singleIntegerRdd = sqrtRdd.map(value -> 1L);
		Long count = singleIntegerRdd.reduce((val1,val2) -> (val1 + val2));
		System.out.println(count);


		// Tuples - ( number, itsSqrt) 

		List<Integer> InputList = new  ArrayList<Integer>();
		InputList.add(25);
		InputList.add(15);
		InputList.add(4);
		InputList.add(9);

		JavaRDD<Integer> originalIntegers = sc.parallelize(InputList); 
		JavaRDD<Tuple2<Integer, Double>> tup_int_sqrt_rdd = originalIntegers.map(value -> new Tuple2<Integer,Double>(value,Math.sqrt(value)));

		tup_int_sqrt_rdd.foreach(value -> System.out.println(value)); 

		// PairRDD  

		List<String> inputDataLog = new  ArrayList<String>();
		inputDataLog.add("WARN: warn message 1");
		inputDataLog.add("ERROR:error message 1 ");
		inputDataLog.add("FATAL:fatal message 1");
		inputDataLog.add("ERROR: error message 2");
		inputDataLog.add("WARN: warn message 2 ");

		JavaRDD<String> baseLogRdd = sc.parallelize(inputDataLog);

		JavaPairRDD<String, String> level_msg_pairRDD = baseLogRdd.mapToPair(record ->  { 

			String[] columns = record.split(":");
			String level =columns[0];
			String msg = columns[1];

			//return new Tuple2<String,String>(level,msg);
			return new Tuple2<>(level,msg);


		});
		
		// groupByKey() usage - recommended not to use groupBy
		
		level_msg_pairRDD.groupByKey()
						 .foreach(tuple -> 
						 System.out.println(tuple._1 + " -- " + Iterables.size(tuple._2) + " instances" ));
		
		

		// reduceByKey()
		// Example: get count by warning level
		// avoiding groupBy and using reduceByKey to get count by logLevel

		JavaPairRDD<String, Long>  pairRdd= baseLogRdd.mapToPair(record ->  { 
			return new Tuple2<>(record.split(":")[0],1L); // <level , 1L > 
		});

		JavaPairRDD<String, Long> logLevel_count = pairRdd.reduceByKey((val1,val2 )-> val1 + val2);


		logLevel_count.foreach(tuple -> {

			System.out.println(tuple._1 + "-- " +tuple._2);
		});
		
		
		// representing all together in one

		sc.parallelize(inputDataLog)
		  .mapToPair(record ->   new Tuple2<>(record.split(":")[0],1L) )
		  .reduceByKey((val1,val2) -> val1+val2)
		  .foreach(tuple -> System.out.println(tuple._1 + "--" + tuple._2));
		
		
		// flatMap & filter 
		
		JavaRDD<String> sentensesRdd = sc.parallelize(inputDataLog);
        JavaRDD<String> wordsRdd = sentensesRdd.flatMap(record -> Arrays.asList(record.split(" ")).iterator());
			
        wordsRdd.filter(word -> word.length() > 1);
        
		wordsRdd.foreach(word-> System.out.println(word));
		
		// flatMap & Filter 
		sc.parallelize(inputDataLog)
		  .flatMap(record -> Arrays.asList(record.split(" ")).iterator())
		  .filter(word -> word.length() > 1)
		  .foreach(word-> System.out.println(word));
		
		
		// Reading from Disk - a text File
		

		 JavaRDD<String> textFileRdd = sc.textFile("");
		 
		
		sc.close();
	}

}
