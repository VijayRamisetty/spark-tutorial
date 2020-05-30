package com.vj.spark.sparksql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import scala.Function1;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.spark.sql.functions.col;


public class SparkSqlAPIWalkthrough {
    public static void main(String[] args) throws AnalysisException {

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession  spark = SparkSession.builder().appName("SparkSQL API Walkthrough").master("local[*]").getOrCreate();

        //DataFrame Creation
        //df.select(col("col_name"))
        //TempView & Global View
        processJsonRecords(spark);

        // Creating Datasets from Bean Object + using Encoder
        // Converting Text File records and mapping to a Bean Object + using Encoder --> creating Dataset<Person>
        processDatasets(spark);

        spark.close();
    }

    private static void processDatasets(SparkSession spark) {
        /**
         * Creating Datasets from Bean Object + using Encoder
         */

        Person p1 = new Person("Vijay",30);
        Person p2 = new Person("Kumar",25);

        // Encoders are created for Java beans
        Encoder<Person> personEncoder = Encoders.bean(Person.class);

        List<Person> personsList = new ArrayList<>();
        personsList.add(p1);
        personsList.add(p2);
        // syntax ---> spark.createDataset(List<Person> ,  Encoder) ;
        // Dataset<Person> javaBeanDS = spark.createDataset( Collections.singletonList(person), personEncoder );
        Dataset<Person> javaBeanDS = spark.createDataset( personsList, personEncoder );
        javaBeanDS.show();

        /**
         * Converting Text File records and mapping to a Bean Object + using Encoder --> creating Dataset<Person>
         */

       Dataset<Row>  personRecords = spark.read().csv("src/main/resources/person.txt");
       personRecords.show();

//+----+---+
//| _c0|_c1|
//+----+---+
//|john| 24|
//|mike| 23|
//| bob| 27|
//+----+---+

       Dataset<Person> x = personRecords.map(new MapFunction<Row, Person>() {
           @Override
           public Person call(Row row) throws Exception {
               return new Person(row.getString(0), (Integer.valueOf(row.getString(1))));
           }
       }, personEncoder);

       x.show();

//+---+----+
//|age|name|
//+---+----+
//| 24|john|
//| 23|mike|
//| 27| bob|
//+---+----+

        // using lambda
        Dataset<Person> y = x.filter((FilterFunction<Person>) obj-> obj.age >23);

//       Dataset<Person> y = x.filter(new FilterFunction<Person>() {
//           @Override
//           public boolean call(Person person) throws Exception {
//               return person.age > 23;
//           }
//       });

       y.show();

//+---+----+
//|age|name|
//+---+----+
//| 24|john|
//| 27| bob|
//+---+----+

    }

    public  static  class Person implements Serializable{
        private String name;
        private int age;

        public Person(String name, int age) {
            this.name = name;
            this.age = age;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }

    private static void processJsonRecords(SparkSession spark ) throws AnalysisException {
        // in real people.json is a text file , with each line having a json record
        Dataset<Row> json_df = spark.read().json("src/main/resources/people.json");
        json_df.show();

// +----+-------+
// | age|   name|
// +----+-------+
// |null|Michael|
// |  30|   Andy|
// |  19| Justin|
// +----+-------+
//
        json_df.printSchema();

// root
//  |-- age: long (nullable = true)
//  |-- name: string (nullable = true)


        json_df.select("name").show();
// +-------+
// |   name|
// +-------+
// |Michael|
// |   Andy|
// | Justin|
// +-------+


        json_df.select(col("name"), col("age").plus(1)).show();
// +-------+---------+
// |   name|(age + 1)|
// +-------+---------+
// |Michael|     null|
// |   Andy|       31|
// | Justin|       20|
// +-------+---------+

// Select people older than 21
        json_df.filter(col("age").gt(21)).show();
// +---+----+
// |age|name|
// +---+----+
// | 30|Andy|
// +---+----+

// Count people by age
        json_df.groupBy("age").count().show();
// +----+-----+
// | age|count|
// +----+-----+
// |  19|    1|
// |null|    1|
// |  30|    1|
// +----+-----+


        // Register the DataFrame as a SQL temporary view
        json_df.createOrReplaceTempView("people");

        Dataset<Row> sql_df= spark.sql("Select * from people");
        sql_df.show();

        // Register the DataFrame as a Global temporary view
        json_df.createGlobalTempView("people");

// Global temporary view is tied to a system preserved database `global_temp`
        spark.sql("SELECT * FROM global_temp.people").show();    // <------- global_temp is system preserved name of global db space
// +----+-------+
// | age|   name|
// +----+-------+
// |null|Michael|
// |  30|   Andy|
// |  19| Justin|
// +----+-------+

// Global temporary view is cross-session
        spark.newSession().sql("SELECT * FROM global_temp.people").show();
// +----+-------+
// | age|   name|
// +----+-------+
// |null|Michael|
// |  30|   Andy|
// |  19| Justin|
// +----+-------+
    }

}
