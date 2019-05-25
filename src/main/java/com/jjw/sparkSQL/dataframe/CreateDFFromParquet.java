package com.jjw.sparkSQL.dataframe;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

public class CreateDFFromParquet {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local").setAppName("parquet");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		JavaRDD<String> jsonRDD = sc.textFile("sparksql/json");
		DataFrame df = sqlContext.read().json(jsonRDD);
//		sqlContext.read().format("json").load("./spark/json");
//		df.show();
		/**
		 * 将DataFrame保存成parquet文件，
		 * SaveMode指定存储文件时的保存模式:
		 * 		Overwrite：覆盖
		 * 		Append:追加
		 * 		ErrorIfExists:如果存在就报错
		 * 		Ignore:如果存在就忽略
		 * 保存成parquet文件有以下两种方式：
		 */
		df.write().mode(SaveMode.Overwrite).format("parquet").save("./sparksql/parquet");
		df.write().mode(SaveMode.Ignore).parquet("./sparksql/parquet");
		/**
		 * 加载parquet文件成DataFrame	
		 * 加载parquet文件有以下两种方式：	
		 */
		
		DataFrame load = sqlContext.read().format("parquet").load("./sparksql/parquet");
//		load = sqlContext.read().parquet("./sparksql/parquet");
		load.show();
		sc.stop();
	}
	
}
