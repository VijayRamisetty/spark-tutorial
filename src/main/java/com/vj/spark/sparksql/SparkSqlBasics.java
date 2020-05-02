package com.vj.spark.sparksql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSqlBasics {

	public static void main(String[] args) {
		
		//System.getProperty("hadoop.home.dir" ,"c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		

		//SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("Spark Word Count");
		//JavaSparkContext  sc = new JavaSparkContext(conf);
		
		
		SparkSession spark = SparkSession.builder()
										 .appName("SparkSQL")
										 .master("local[*]")
										 //.config("spark.sql.warehouse.dir","files://c:/tmp/")
										 .getOrCreate();
		
		Dataset<Row> csvDataset = spark.read()
			 .option("header", true)
			 .csv("src/main/resources/students.csv");
		
		// show
		csvDataset.show();
		
		// count
		long numRows = csvDataset.count();
		System.out.println("number of rows in csv :"+ numRows);
		
		// first
		Row first = csvDataset.first();
		System.out.println(first);
		
		// first.get(i)
		String subject = first.get(2).toString();
		System.out.println(subject);
		
		// when header True, first.getAs(colName)
		String subj =  first.getAs("subject").toString();
		System.out.println(subj);
		
		// applying filter on dataset using - conditionExpr
		Dataset<Row> mathDataset = csvDataset.filter("subject = 'Math' AND year > 2006 ");
		mathDataset.show();
		
		// filter -  using lambda
		Dataset<Row> _mathDataset = csvDataset.filter(row -> 
			row.getAs("subject").equals("Math") && Integer.parseInt(row.getAs("year")) > 2006
		);
		
		_mathDataset.show();
		
		// filter by columns
		Dataset<Row> mathDataSet2 = csvDataset.filter( 
				csvDataset.col("subject").equalTo("Math").and(csvDataset.col("year").gt(2006)));
		
		mathDataSet2.show();
		
		
		
		spark.close();
										 
	}

}
