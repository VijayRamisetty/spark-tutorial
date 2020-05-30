package com.vj.spark.sparksql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

import java.util.ArrayList;
import java.util.List;


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
		Dataset<Row> mathDataset1 = csvDataset.filter((FilterFunction<Row>) row ->
			row.getAs("subject").equals("Math") && Integer.parseInt(row.getAs("year")) > 2006
		);


		mathDataset1.show();
		
		// filter using columns - approach1
		Column subjectCol = csvDataset.col("subject");
		Column yearCol = csvDataset.col("year");
							
		Dataset<Row> mathDataSet2 = csvDataset.filter(subjectCol.equalTo("Math")
														 .and(yearCol.geq(2007)));
		mathDataSet2.show();
		
		// filter using  columns - approach2
		Dataset<Row> mathDataSet3 = csvDataset.filter( 
						csvDataset.col("subject").equalTo("Math").and(csvDataset.col("year").gt(2007)));
				
		mathDataSet3.show();
		
		// filter using  - Class  org.apache.spark.sql.'f'unctions - static methods
		
		Column subjCol = functions.col("subject");
		Column yrCol = functions.col("year");
		
		Dataset<Row> mathDataSet4 = csvDataset.filter(subjectCol.equalTo("Math")
				 .and(yearCol.geq(2007)));
		mathDataSet4.show();
		
		// more simplest way
		// filter using  - import static org.apache.spark.sql.functions.*;

		Dataset<Row> mathDataSet5 = csvDataset.filter(col("subject").equalTo("Math")
				 .and(col("year").geq(2007)));
		mathDataSet5.show();
		
		
		// filter using spark temp table view
		
		csvDataset.createOrReplaceTempView("students_tbl");
		
		Dataset<Row> mathDataSet6 = spark.sql("SELECT * from students_tbl where subject ='Math' and year > 2007");
		
		mathDataSet6.show();
	
		//aggregations
		
		csvDataset.groupBy("subject")
				  .agg(max(col("score")).alias("maxscore") ,
						  min(col("score")).alias("minscore"))
				  .show();
		
		// adding new column 
		csvDataset.withColumn("pass", lit(col("grade").equalTo("A+")))
			.show();
		
		// using udf
		
		spark.udf().register("haspassed", (String grade) ->  grade.equals("A+") ,DataTypes.BooleanType );
		
		csvDataset.withColumn("pass", callUDF("haspassed", col("grade")) )
				  .show();
		spark.close();
		
		
		
										 
	}

}
