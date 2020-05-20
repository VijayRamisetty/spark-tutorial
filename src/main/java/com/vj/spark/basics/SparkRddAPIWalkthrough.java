package com.vj.spark.basics;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

/**
 *  
 *  Parallelized Collections
 *  passing functions in multiple ways
 *  Accumulator
 * 
 */

public class SparkRddAPIWalkthrough {
	
    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf= new SparkConf().setAppName("SparkRddAPIWalkthrough").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        //Parallelized Collections

        List<Integer> list =  Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> distData = jsc.parallelize(list);

        Integer sum = distData.reduce((a, b) -> a + b);
        sysout(sum);


        // read file
        JavaRDD<String> lines = jsc.textFile("data.txt");

        // using lambda expressions

        JavaRDD<Integer> lineLengths = lines.map(x -> x.length());
        int totalLength =lineLengths.reduce((a,b)->a+b);
        sysout(totalLength);


        // using Functions - anonymous inner class

        JavaRDD<Integer> lineLengths2 =  lines.map(new Function<String, Integer>() {
            @Override
            public Integer call(String s) throws Exception {
                return  s.length();
            }
        });

       int totalLength2 =  lineLengths2.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer a, Integer b ) throws Exception {
                return  a +b ;
            }
        });

       sysout(totalLength2);


       // using - implementing Function Interfaces in our own class

        JavaRDD<Integer> lineLengths3 = lines.map(new GetLength());
        int totalLength3 =lineLengths.reduce(new Sum());
        sysout(totalLength3);


        // Accumulator


        LongAccumulator longAccum = jsc.sc().longAccumulator();

        lines.repartition(2).foreach( x-> {
            longAccum.add(1);
        });

        sysout(longAccum.value());

        Scanner sc = new Scanner(System.in);
        sc.nextLine();
        jsc.close();
    }

    //implementing Function Interfaces in our own class

    static class GetLength implements  Function<String,Integer>{
        @Override
        public Integer call(String s) throws Exception {
            return s.length();
        }
    }
    static class Sum implements Function2<Integer ,Integer,Integer>{

        @Override
        public Integer call(Integer a, Integer b) throws Exception {
            return a+b;
        }
    }

    // util for print to replace sysout.
    public static void sysout(Object obj){
        System.out.println(obj);
    }
}

/*
 

15
23630
23630
23630
70

*/

