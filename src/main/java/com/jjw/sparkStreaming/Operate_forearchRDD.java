package com.jjw.sparkStreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * forearchRDD是output类算子
 * 对于从流中RDD应用func的最通用的一个output 操作。
 * 该功能应将每个RDD中的数据推送到外部系统，例如将RDD保存到文件，或将其通过网络写入数据库。
 * 
 * 注意：如果使用foreachRDD这个算子，必须要对抽取出来的RDD执行action类算子，代码才能正常执行。
 * @author root
 *
 */
public class Operate_forearchRDD {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Operate_forearchRDD");
		JavaStreamingContext jsc = new JavaStreamingContext(conf,Durations.seconds(5));
		JavaDStream<String> textFileStream = jsc.socketTextStream("node5", 9999);
		
		textFileStream.foreachRDD(new VoidFunction<JavaRDD<String>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public void call(JavaRDD<String> rdd) throws Exception {
				rdd.foreach(new VoidFunction<String>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					public void call(String s) throws Exception {
						System.out.println(s);
						
					}
				});
			}
		});
		
		jsc.start();
		jsc.awaitTermination();
		jsc.close();
	}
}
