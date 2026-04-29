package com.example;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


public class App 
{
    public static void main(String[] args)
    {
        SparkSession spark = SparkSession.builder().appName("Spark Hello World").master("local[*]").getOrCreate();

        System.out.println("Hello, World");

        Dataset<Row> df = spark.read().json(spark.emptyDataset(org.apache.spark.sql.Encoders.STRING()));

        df.show();

        spark.stop();

    }
}



