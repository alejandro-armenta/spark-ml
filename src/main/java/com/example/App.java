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
        SparkSession spark = SparkSession.builder().appName("Restaurants in Wake County, NC").master("local[*]").getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        Dataset<Row> df = spark.read().format("csv").option("header", "true").load("Restaurants_in_Wake_County_NC.csv");        

        df.show();

        df.printSchema();

        System.out.println("RECORDS: " + df.count());


        df = df.
        drop(col("OBJECTID")).
        drop(col("PERMITID")).
        drop(col("GEOCODESTATUS")).
        withColumn("county", lit("Wake")).
        withColumnRenamed("HSISID", "datasetId").
        withColumnRenamed("NAME", "name").
        withColumnRenamed("ADDRESS1", "address1").
        withColumnRenamed("ADDRESS2", "address2").
        withColumnRenamed("CITY", "city").
        withColumnRenamed("STATE", "state").
        withColumnRenamed("POSTALCODE", "zip").
        withColumnRenamed("PHONENUMBER", "tel").
        withColumnRenamed("RESTAURANTOPENDATE", "dateStart").
        withColumnRenamed("FACILITYTYPE", "type").
        withColumnRenamed("X", "geoX").
        withColumnRenamed("Y", "geoY")
        ;

        df.show();

        df.
        withColumn(
            "id", 
            concat(
                col("state"), lit("_"), col("county"), lit("_"), col("datasetId")
            )
        ).
        show();


    }
}



