package com.vj.spark.sparksql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;


/**
 * 
 * @author rami
 *
 */
public class SparkSqlAdvanced {

	public static void main(String[] args) {
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession spark = SparkSession.builder().appName("SparkSqlAdvanced")
								.master("local[*]")
								.getOrCreate();
		
		
		// creating inMemory dataset using RowFactory
		
		// list of Rows
		List<Row> inMemory = new ArrayList<Row>();
		inMemory.add(RowFactory.create("WARN","2016-12-31 04:19:32"));
	    inMemory.add(RowFactory.create("FATAL","2016-12-31 03:22:34"));
	    inMemory.add(RowFactory.create("WARN","2016-12-31 03:21:21"));
	    inMemory.add(RowFactory.create("INFO","2015-4-21 14:32:21"));
	    inMemory.add(RowFactory.create("FATAL","2015-4-21 19:23:20"));

	    // fields
		StructField[] fields = new StructField[]  {
				new StructField("level",DataTypes.StringType,false,Metadata.empty()),
				new StructField("datetime",DataTypes.StringType,false,Metadata.empty())
		};
		
		// schema
		StructType schema = new StructType(fields );
		
		// schema + listOfRows
		Dataset<Row> dataset = spark.createDataFrame(inMemory,schema);
		dataset.show();
				
		dataset.createOrReplaceTempView("logging_tbl");
		
		// below will not work
		// group by should always comes with an aggregation
		// Dataset<Row> results = spark.sql("select level,datetime from logging_tbl group by level");
		
		Dataset<Row> results = spark.sql("select level,count(datetime) from logging_tbl group by level ");
		// like count(on a column)  more available at 
		// https://spark.apache.org/docs/latest/ -> API docs -> SQL Built-in Functions

		
		// Date formatting - more formats @ java.text.SimpleDataFormat
		Dataset<Row> results2= spark.sql("select level,date_Format(datetime,'MMM') as month from logging_tbl ");
		results2.show();
		
		// using selectExpr
		
		Dataset<Row> dataset_new = dataset.selectExpr("level","date_Format(datetime,'MMM') as month");
		dataset_new.show();
		
		// using functions col
		Dataset<Row> dataset_new2 = dataset.select(col("level"),date_format(col("datetime"),"MMM").alias("month"));
		dataset_new2.show();
		
		// we can apply groupby & orderby also
		dataset_new2 = dataset_new2.groupBy(col("level"),col("month")).count();
		dataset_new2 =dataset_new2.orderBy(col("level"),col("month"));
		dataset_new2.show();
		
		// using pivot
		//na().fill(0) when no matches instead of nulls
		
		dataset_new2.groupBy("level").pivot("month").count().na().fill(0)
		.show();
		
		// pivot() can take second arg a list, on which lis of cols to apply pivot

		Object[] months = new Object[] {"Dec" };
		List<Object> columns = Arrays.asList(months);
		
		dataset_new2.groupBy("level").pivot("month",columns).count().na().fill(0)
		.show();
		
		
		
		spark.close();
		
	}
}
