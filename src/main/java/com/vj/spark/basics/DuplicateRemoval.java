package com.vj.spark.basics;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * How to remove duplicate records using spark. Take Name & Age as key
 */
public class DuplicateRemoval {

    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("Test1");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        List<String> records = new ArrayList<>();
        records.add("Rajesh,21,London");
        records.add("Suresh,28,California");
        records.add("Sam,21,Delhi");
        records.add("Rajesh,21,Gurugaon");      // duplicate
        records.add("Manish,21,Bengaluru");

        JavaRDD<String> baseRdd = jsc.parallelize(records);
        JavaPairRDD<String, String> nameAge_loc_rdd = baseRdd.mapToPair(x -> {

            String[] a = x.split(",");
            String name = a[0];
            String age = a[1];
            String location = a[2];
            return new Tuple2<>(name + "" + age, location);

        });

        nameAge_loc_rdd =  nameAge_loc_rdd.reduceByKey((x,y)-> x);

        System.out.println(nameAge_loc_rdd.collect());

        jsc.close();
    }
}

/**
 *
 * Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
 * 20/05/29 21:26:16 WARN Utils: Your hostname, Vijays-MacBook-Air.local resolves to a loopback address: 127.0.0.1; using 192.168.1.22 instead (on interface en0)
 * 20/05/29 21:26:16 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to another address
 * 20/05/29 21:26:16 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
 * [(Suresh28,California), (Sam21,Delhi), (Rajesh21,London), (Manish21,Bengaluru)]
 */

