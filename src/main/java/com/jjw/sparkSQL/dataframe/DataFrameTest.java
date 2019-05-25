package com.jjw.sparkSQL.dataframe;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import scala.Function1;
import scala.runtime.BoxedUnit;

public class DataFrameTest {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local").setAppName("RDD");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		JavaRDD<String> lineRDD = sc.textFile("sparksql/person.txt");
		
		JavaRDD<Person> personRDD = lineRDD.map(new Function<String, Person>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Person call(String line) throws Exception {
				Person p = new Person();
				p.setId(line.split(",")[0]);
				p.setName(line.split(",")[1]);
				p.setAge(Integer.valueOf(line.split(",")[2]));
				return p;
			}
		});
		/**
		 * 传入进去Person.class的时候，sqlContext是通过反射的方式创建DataFrame
		 * 在底层通过反射的方式获得Person的所有field，结合RDD本身，就生成了DataFrame
		 */
		DataFrame df = sqlContext.createDataFrame(personRDD, Person.class);
		df.show();
		df.printSchema();
		df.registerTempTable("person");
		DataFrame resultDataFrame = sqlContext.sql("select  name,age,id from person where id = 2");
		JavaRDD<Row> javaRDD = resultDataFrame.javaRDD();
		/**
		 * 自己写的sql语句查询出来的DataFrame显示表的时候会安装查询的字段来显示，字段不会按照Ascii码来排序
		 */
		javaRDD.foreach(new VoidFunction<Row>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Row row) throws Exception {
				System.out.println("name = "+ row.getAs(0));
				System.out.println("name = "+ row.getAs("name"));
				System.out.println("name = "+ row.getString(0));
				System.out.println("age = "+ row.getAs(1));
				System.out.println("age = "+ row.getAs("age"));
				System.out.println("age = "+ row.getInt(1));
				System.out.println("id = "+ row.getAs(2));
				System.out.println("id = "+ row.getAs("id"));
				System.out.println("id = "+ row.getString(2));
			}
		});
//		/**
//		 * 将DataFrame转成JavaRDD
//		 * 注意：
//		 * 1.可以使用row.getInt(0),row.getString(1)...通过下标获取返回Row类型的数据，但是要注意列顺序问题---不常用
//		 * 2.可以使用row.getAs("列名")来获取对应的列值。
//		 * 
//		 */
//		JavaRDD<Row> javaRDD = df.javaRDD();
//		JavaRDD<Person> map = javaRDD.map(new Function<Row, Person>() {
//
//			/**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Person call(Row row) throws Exception {
//				Person p = new Person();
//				
//				
////				p.setId(row.getString(0));
////				p.setName(row.getString(1));
////				p.setAge(row.getInt(2));
//				
////				p.setId(row.getString(1));
////				p.setName(row.getString(2));
////				p.setAge(row.getInt(0));
//				
//				p.setId((String)row.getAs("id"));
//				p.setName((String)row.getAs("name"));
//				p.setAge((Integer)row.getAs("age"));
//				return p;
//			}
//		});
//		map.foreach(new VoidFunction<Person>() {
//			
//			/**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public void call(Person t) throws Exception {
//				System.out.println(t);
//			}
//		});
		
		sc.stop();
	}
}
