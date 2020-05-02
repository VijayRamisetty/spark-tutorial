CHAPTER-1 : Introduction
===============================
-	Spark RDD
-	Spark SQL & DataFrame
-	Spark ML 
-	Spark Streaming + Apache Kafka

(Java8) 



Hadoop - Limitations
---------------------

-  Map & Reduce  not sutiable for all cases
- output of one fed as input to another 

Spark 
------
- also uses M & R
- 10x faster on disk 
- 100x faster in memory
- Build Execution plan DAG ( Directed Acyclic Graph ) 
- Clever optimization , ex: parallel running of non dependent tasks
- Benefit of MultiThreading

- Driver ( sends functions to )    +  worker1  + worker2 + ...... 
	- task : function against a partition 


RDD
---
- Resilient Distributed Dataset
- Resilient = on failure , data is reconstructed by lineage
- RDD don'ts exists first - instead - as execution Plan 
- RDD immutable

Map
reduce
Pairs
Tuples
Flatmaps
Sorting


CHAPTER-2 : Setup of eclipse & launching App locally  
=====================================================

eclipse 
- ensure Java8
- maven project - pom.xml
- maven build - goal - eclipse:eclipse

	<dependencies>
			<dependency>
				<groupId>org.apache.spark</groupId>
				<artifactId>spark-core_2.10</artifactId>
				<version>2.0.0</version>
			</dependency>
			<dependency>
				<groupId>org.apache.spark</groupId>
				<artifactId>spark-sql_2.10</artifactId>
				<version>2.0.0</version>
			</dependency>
			<dependency>
				<groupId>org.apache.hadoop</groupId>
				<artifactId>hadoop-hdfs</artifactId>
				<version>2.2.0</version>
			</dependency>
	</dependencies>


Local
-----

	Logger.getLogger("org.apache").setLevel(Level.WARN);;
			
	SparkConf conf = new SparkConf()
								.setAppName("App_JavaCollectionToRdd")
								.setMaster("local[*]");
			
	JavaSparkContext sc = new JavaSparkContext(conf);





CHAPTER-3 :  Map & Reduce
==========================

reduce()
--------
	note: Output Type of reducer is same as input Type
    
    Double result = baseRdd.reduce((Double val1, Double val2) -> val1+val2);
	Double result = baseRdd.reduce(( val1, val2) -> val1+val2);

	Node-1
	7
	4
	9
	13
	
	Node-2
	8
	2
	7
	1
	
	Node-3
	9
	4
	3
	2
	
	
	Driver -> sending [ function = val1 + val2]   ->  Node-1
	
	so in Node-1       Node-1
	7= value1          
	4= value2      -->   11                  11 = value1
	9                     9  = value1   -->  22 = value2 -> 33 
	13                    13 = value2

spark nominates any two values 

- this happens in all 3 nodes 
	- sub totals sent to Node x
		- again function = val1 + val2 applied 
		
		

map()
----- 

	// function = sqrt(value)

	JavaRDD<Double> sqrtRdd = baseRdd.map( value -> Math.sqrt(value));

CHAPTER 4 : COUNT
=================

count()
-------
	sqrtRdd.count()


count by map & reduce
--------------------

	a            1
	b  -> map -> 1  -> reduce -> 3  
	c            1


CHAPTER 5 : Tuples
=================

Tuple : 

-	Spark Specific (concept from Scala)
- 	small collection of values that we are not planning for modifying
-	*Tuple2..... to ... Tuple22


Example : (Integer, SqrtValue(Integer))


	import scala.Tuple2;

	List<Integer> InputList = new  ArrayList<Integer>();
		InputList.add(25);
		InputList.add(15);
		InputList.add(4);
		InputList.add(9);
		
	JavaRDD<Integer> originalIntegers = sc.parallelize(InputList); 
	JavaRDD<Tuple2<Integer, Double>> tup_int_sqrt_rdd = originalIntegers.map(value -> new Tuple2<Integer,Double>(value,Math.sqrt(value)));
		
	tup_int_sqrt_rdd.foreach(value -> System.out.println(value)); 

CHAPTER 6 : PairRDDs
====================


	JavaPairRDD<String, String> level_msg_pairRDD = baseLogRdd.mapToPair(record ->  { 
			
			String[] columns = record.split(":");
			String level =columns[0];
			String msg = columns[1];
			
			//return new Tuple2<String,String>(level,msg);
			return new Tuple2<>(level,msg);
		});


GroupBy Key
-----------

*Group by Key can lead to severe performance problems( sometime even crash ) 

-	if groupByKey is used 
	- for a given key , all values needs to be collected from all nodes to single node
	

when groupbyKey() applied on ** PairRdd<String,String>** , a Transformation will occur creating  ** PairRdd<String, Iterable<String>> **

example:  

	Key  Value
	---- ------
	WARN  [ msg1 , msg2 , msg3 .... ]
	

// groupByKey() usage - recommended not to use groupBy
		
	level_msg_pairRDD.groupByKey()
						 .foreach(tuple -> 
						 System.out.println(tuple._1 + " -- " + Iterables.size(tuple._2) + " instances" ));
		 

ReduceBy Key
------------
 	
	pairRdd.reduceBykey( (val1 ,value2) -> val1 + val2 )


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


CHAPTER 7 : FlatMaps & Filters
==============================


-	FlatMap - zero or more outputs 
-	Filters - .filter(x -> condition) , if true consider else ignore


	sc.parallelize(inputDataLog)
		  .flatMap(record -> Arrays.asList(record.split(" ")).iterator())
		  .filter(word -> word.length() > 1)
		  .foreach(word-> System.out.println(word));


CHAPTER 8 : Reading Files
==============================		  

	sc.textFile("/path/to/hdfs/")
sc.textFile 
- not loads the file to client , instead tells all workers in nodes to load respective partitions of file


Spark Word Count
-----------------


	sc.textFile("src/main/resources/input/input.txt")
	   .flatMap(x->  Arrays.asList(x.split(" ")).iterator())
	   .map(x-> x.replaceAll("[^a-zA-Z]", ""))
	   .filter(x-> x.length() > 5)
	   .mapToPair(x-> new Tuple2<String,Long>(x,1L))
	   .reduceByKey((a,b)->(a+b))
	   .saveAsTextFile("src/main/resources/output");

CHAPTER 9  : More on Transformations & Actions
==============================================

https://spark.apache.org

Transformations
---------------
	map
	filter
	flatMap
	groupByKey * 
	reduceByKey *
	sortByKey *
	join
Actions
---------------
	reduce
	collect
	count
	first
	take(n)
	saveAsTextFile(path)
	foreach(func)
	countByKey() *


sortByKey
---------

// sortBykey

		System.out.println("------------------------------------");
		List<String> inputSample = new  ArrayList<String>();
		inputSample.add("hyd chn chn bnglr hyd");
		inputSample.add("bnglr chn hyd hyd bnglr ");
		inputSample.add("bnglr bnglr chn");
		inputSample.add("hyd hyd chn chn bnglr ");
		inputSample.add("hyd chn chn hyd");

		JavaPairRDD<String, Long>  city_count = sc.parallelize(inputSample)
													.flatMap(x-> Arrays.asList(x.split(" ")).iterator())
													.mapToPair(x-> new Tuple2<String,Long>(x,1L))
													.reduceByKey((x,y)-> (x+y));
		city_count.sortByKey()
					.foreach(x-> System.out.println(x));

// sort by value 
-	( flip **city_count to count_city** - using mapToPair ) 

		System.out.println("------------------------------------");
		JavaPairRDD<Long, String> count_city = city_count.mapToPair(x-> new Tuple2<Long,String>(x._2,x._1));
		count_city.sortByKey(false)
					.foreach(x-> System.out.println(x));


CHAPTER 10 - Sort & Coalesce
=============================
