package com.example;

import org.apache.spark.Partition;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.to_date;


import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.text.SimpleDateFormat;


import com.example.Book;

public class App implements Serializable
{
    private static final long serialVersionUID = -1L;

    class BookMapper implements MapFunction<Row, Book>
    {
        private static final long serialVersionUID = -2L;

        @Override
        public Book call(Row value) throws Exception
        {
            Book b = new Book();

            b.setId(value.getAs("id"));
            b.setAuthorId(value.getAs("authorId"));
            b.setLink(value.getAs("link"));
            b.setTitle(value.getAs("title"));

            //date
            String dateAsString = value.getAs("releaseDate");

            if (dateAsString != null)
            {
                SimpleDateFormat parser = new SimpleDateFormat("M/d/yy");

                b.setReleaseDate(parser.parse(dateAsString));
            }




            return b;
        }

    }
    
    public static void main(String[] args)
    {
        App app = new App();
        app.start();
    }

    private void start()
    {
        SparkSession spark = SparkSession.builder()
        .appName("CSV to dataframe to Dataset<Book> and back")
        .master("local")
        .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");
        
        spark.conf().set("spark.sql.legacy.timeParserPolicy", "LEGACY");

        Dataset<Row> df = spark.
        read().
        format("csv").
        option("inferSchema", "true").
        option("header", "true").
        load("books.csv")
        ;

        df.show();
        df.printSchema();

        Dataset<Book> bookDs = df.map(
            new BookMapper(), Encoders.bean(Book.class));

        bookDs.show();
        bookDs.printSchema();

        Dataset<Row> df2 = bookDs.toDF();

        df2 = df2.
        withColumn(
            "releaseDateAsString",
            concat(
                expr("releaseDate.year + 1900"),
                lit("-"),
                expr("releaseDate.month + 1"),
                lit("-"),
                col("releaseDate.date")
            )
        );

        /*
        df2.withColumn(
            "releaseDateAsDate",
            to_date(
                df2.col("releaseDateAsString"),"yyyy-MM-dd"
            )
        ).show();
        */

        


    }

    
}



