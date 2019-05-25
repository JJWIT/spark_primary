package com.jjw.sparkSQL.dataframe;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
/**
 * 读取json格式的文件创建DataFrame
 * 
 * 注意 ：json文件中不能嵌套json格式的内容
 * 
 * 1.读取json格式两种方式
 * 2.df.show默认显示前20行，使用df.show(行数)显示多行
 * 3.df.javaRDD/(scala df.rdd) 将DataFrame转换成RDD
 * 4.df.printSchema()显示DataFrame中的Schema信息
 * 5.dataFram自带的API 操作DataFrame ，用的少
 * 6.想使用sql查询，首先要将DataFrame注册成临时表：df.registerTempTable("jtable")，再使用sql,怎么使用sql?sqlContext.sql("sql语句")
 * 7.不能读取嵌套的json文件
 * 8.df加载过来之后将列按照ascii排序了
 * @author root
 *
 */
public class CreateDFFromJosonFile {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local").setAppName("jsonfile");
		SparkContext sc = new SparkContext(conf);
		
		//创建sqlContext
		SQLContext sqlContext = new SQLContext(sc);
		
		/**
		 * DataFrame的底层是一个一个的RDD  RDD的泛型是Row类型。
		 * 以下两种方式都可以读取json格式的文件
		 */
		DataFrame df = sqlContext.read().format("json").load("./sparksql/json");
//		DataFrame df2 = sqlContext.read().json("sparksql/json");
//		df2.show();
		/**
		 * 显示 DataFrame中的内容，默认显示前20行。如果现实多行要指定多少行show(行数)
		 * 注意：当有多个列时，显示的列先后顺序是按列的ascii码先后显示。
		 */
		df.show(100);
		/**
		 * DataFrame转换成RDD
		 */
//		JavaRDD<Row> javaRDD = df.javaRDD();
		/**
		 * 树形的形式显示schema信息
		 */
//		df.printSchema();
		
		/**
		 * dataFram自带的API 操作DataFrame
		 */
		//select name from table
//		df.select("name").show();
		//select name ,age+10 as addage from table
//		df.select(df.col("name"),df.col("age").plus(10).alias("addage")).show();
		//select name ,age from table where age>19
//		df.select(df.col("name"),df.col("age")).where(df.col("age").gt(19)).show();
		//select age,count(*) from table group by age
//		df.groupBy(df.col("age")).count().show();
		
		/**
		 * 将DataFrame注册成临时的一张表，这张表相当于临时注册到内存中，是逻辑上的表，不会雾化到磁盘
		 */
//		df.registerTempTable("jtable");
//		DataFrame sql = sqlContext.sql("select age,count(*) as gg from jtable group by age");
//		sql.show();
//		DataFrame sql2 = sqlContext.sql("select name,age from jtable");
//		sql2.show();
		sc.stop();
	}
}
