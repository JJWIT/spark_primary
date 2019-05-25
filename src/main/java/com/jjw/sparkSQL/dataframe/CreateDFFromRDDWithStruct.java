/**
 * 
 */
package com.jjw.sparkSQL.dataframe;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * 动态创建Schema将非json格式RDD转换成DataFrame
 * @author root
 *
 */
public class CreateDFFromRDDWithStruct {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local").setAppName("rddStruct");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		JavaRDD<String> lineRDD = sc.textFile("./sparksql/person.txt");
		/**
		 * 转换成Row类型的RDD
		 */
		JavaRDD<Row> rowRDD = lineRDD.map(new Function<String, Row>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Row call(String s) throws Exception {
				return RowFactory.create(
						s.split(",")[0],
						s.split(",")[1],
						Integer.valueOf(s.split(",")[2]
					));
			}
		});
		/**
		 * 动态构建DataFrame中的元数据，一般来说这里的字段可以来源自字符串，也可以来源于外部数据库
		 */
		List<StructField> asList =Arrays.asList(
					DataTypes.createStructField("id", DataTypes.StringType, true),
					DataTypes.createStructField("name", DataTypes.StringType, true),
					DataTypes.createStructField("age", DataTypes.IntegerType, true)
		);
		
		StructType schema = DataTypes.createStructType(asList);
		
		DataFrame df = sqlContext.createDataFrame(rowRDD, schema);
		
		df.show();
//		JavaRDD<Row> javaRDD = df.javaRDD();
//		javaRDD.foreach(new VoidFunction<Row>() {
//			
//			/**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public void call(Row row) throws Exception {
//				System.out.println(row.getString(0));
//			}
//		});
		sc.stop();
		
	}
}
