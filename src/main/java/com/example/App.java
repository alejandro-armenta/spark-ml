package com.example;

import org.apache.spark.Partition;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import java.sql.Struct;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.split;

public class App 
{
    private SparkSession spark;
    public static void main(String[] args)
    {
        App app = new App();
        app.start();
    }

    private void start()
    {
        this.spark = SparkSession.builder().appName("Restaurants in Wake County, NC").master("local[*]").getOrCreate();

        this.spark.sparkContext().setLogLevel("WARN");

        Dataset<Row> wake = buildWake();

        wake.show();

        Dataset<Row> durham = buildDurham();

        Dataset<Row> df = wake.unionByName(durham);

        df.printSchema();

        System.out.println(wake.count() + durham.count());
        System.out.println(df.count());

    }

    private Dataset<Row> buildWake()
    {
        Dataset<Row> df = this.spark.read().format("csv").option("header", "true").load("Restaurants_in_Wake_County_NC.csv");        

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
        withColumn("dateEnd", lit(null)).
        withColumnRenamed("FACILITYTYPE", "type").
        withColumnRenamed("X", "geoX").
        withColumnRenamed("Y", "geoY")
        ;

        
        df = df.
        withColumn(
            "id", 
            concat(
                col("state"), lit("_"), col("county"), lit("_"), col("datasetId")
            )
        );

        return df;

    }

    private Dataset<Row> buildDurham()
    {
        Dataset<Row> df = this.spark.read().format("json").load("Restaurants_in_Durham_County_NC.json");        

        df.show();

        df.
        select(
            col("fields.geolocation").getItem(0)
        ).
        show()
        ;
        
        
        df = df.
        withColumn("county", lit("Durham")).
        withColumn("datasetId",col("fields.id")).
        withColumn("name",col("fields.premise_name")).
        withColumn("address1",col("fields.premise_address1")).
        withColumn("address2",col("fields.premise_address2")).
        withColumn("city",col("fields.premise_city")).
        withColumn("state",col("fields.premise_state")).
        withColumn("zip",col("fields.premise_zip")).
        withColumn("tel",col("fields.premise_phone")).
        withColumn("dateStart",col("fields.opening_date")).
        withColumn("dateEnd",col("fields.closing_date")).
        withColumn("type",
            split(col("fields.type_description"), " - ").getItem(1)
        ).
        withColumn("geoX",
            col("fields.geolocation").getItem(0)
        ).
        withColumn("geoY",
            col("fields.geolocation").getItem(1)
        ).
        drop(col("fields")).
        drop(col("geometry")).
        drop(col("record_timestamp")).
        drop(col("recordid"))
        ;


        df = df.
        withColumn(
            "id", 
            concat(
                col("state"), lit("_"), col("county"), lit("_"), col("datasetId")
            )
        );
        
        df.show();

        return df;

    }
}



