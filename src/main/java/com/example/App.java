package com.example;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.col;


public class App 
{
    public static void main(String[] args)
    {
        SparkSession spark = SparkSession.builder().appName("CSV TO DB").master("local[*]").getOrCreate();

        Dataset<Row> df = spark.read().format("csv").option("header", "true").load("authors.csv");

        
        df.select(col("lname"), col("fname")).show();
        
        df.select(
            concat(
                col("lname"), lit(","), col("fname")
            )
        ).show();

        df = df.withColumn(
            "ale", 
            concat(
                col("lname"), lit(","), col("fname")
            )
        );


        df.show(5);
        
        
        spark.stop();

    }
}



