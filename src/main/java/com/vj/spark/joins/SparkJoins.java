package com.vj.spark.joins;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;

import scala.Tuple2;

/**
 * 
 * @author rami
 * 
 * JOINS 
 * 
 */
public class SparkJoins {

	public static void main(String[] args) {
		
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkConf conf = new SparkConf().setMaster("local[*]")
										.setAppName("Spark Word Count");
		JavaSparkContext  sc = new JavaSparkContext(conf);

		
		// user_id , visits
		List<Tuple2<Integer,Integer>> user_visits = new ArrayList<>();
		user_visits.add(new Tuple2<>(4,18));
		user_visits.add(new Tuple2<>(6,4));
		user_visits.add(new Tuple2<>(10,9));
	
		// user_id, name
		List<Tuple2<Integer,String>> user_name = new ArrayList<>();
		user_name.add(new Tuple2<>(1,"John"));
		user_name.add(new Tuple2<>(2,"Bob"));
		user_name.add(new Tuple2<>(3,"Alice"));
		user_name.add(new Tuple2<>(4,"Doris"));
		user_name.add(new Tuple2<>(5,"Marybelle"));
		user_name.add(new Tuple2<>(6,"Raquel"));

		JavaPairRDD<Integer, Integer> user_visit_rdd = sc.parallelizePairs(user_visits);
		JavaPairRDD<Integer, String> user_name_rdd  = sc.parallelizePairs(user_name); 

		
		System.out.println("---- JOIN ( InnerJoin) -----");
		//<user, <visits,name>>    note: Join itself is InnerJoin
		JavaPairRDD<Integer, Tuple2<Integer, String>> joinedRdd  = user_visit_rdd.join(user_name_rdd);

		joinedRdd.foreach(x-> System.out.println(x));
		
		
		
		System.out.println("----(LEFT) Outer JOIN  -----");
		
		//<user, <visits,Optinal<name>>>    
		 JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> leftOuterJoinRdd = user_visit_rdd.leftOuterJoin(user_name_rdd);

		 leftOuterJoinRdd.foreach(x-> System.out.println(x));
		 
		 
		System.out.println("----handling optionals -----");

		 // how to handle Optionals
		 
		 
		 leftOuterJoinRdd.foreach(x-> {
			 Integer user_id = x._1;
			 Integer views =x._2._1;
			 String  name = x._2._2.orElse("BLANK");  // x._2._2.isPresent() can also be used
			 
			 System.out.println( user_id + " " + views + " "  + name);
		 }
		);
		 
		 
	   System.out.println("----(Right) Outer JOIN  -----");
	   
	   
	   
	   JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> rightOuterJoinRdd = user_visit_rdd.rightOuterJoin(user_name_rdd);

	   rightOuterJoinRdd.foreach(x-> System.out.println(x));
	   
	   // how to handle Optionals
		 
		 
	   rightOuterJoinRdd.foreach(x-> {
			 Integer user_id = x._1;
			 Integer views =x._2._1.orElse(0); // setting 0 in case of no matching
			 String  name = x._2._2;  
			 
			 System.out.println( user_id + ")"  + name + " had " + views +" views " );
		 }
	   );
	   
	   
	   
	   System.out.println("----Full Outer JOIN  -----");
	   
	   JavaPairRDD<Integer, Tuple2<Optional<Integer>, Optional<String>>> fullOuterJoin = user_visit_rdd.fullOuterJoin(user_name_rdd);
		
	   fullOuterJoin.foreach(x-> System.out.println(x));
	   
	   
	   System.out.println("----Cartesian JOIN  -----");
	   JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, String>> cartesianJoin = user_visit_rdd.cartesian(user_name_rdd);
	   
	   cartesianJoin.foreach(x-> System.out.println(x));
	   
	   sc.close();
		
	}

	
}
