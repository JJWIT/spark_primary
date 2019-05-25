package com.jjw.sparkStreaming;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.google.common.base.Optional;

import scala.Tuple2;

/**
 * 过滤黑名单（使用广播变量）
 * 
 * @author root
 *
 */
public class TransformOperator2 {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local[2]").setAppName("transform");
		JavaStreamingContext jsc = new JavaStreamingContext(conf,Durations.seconds(5));
		
		//模拟黑名单
		List<String> blackList = new ArrayList<String>();
		blackList.add("zhangsan");
		//广播黑名单	
		final Broadcast<List<String>> broadcastList = jsc.sparkContext().broadcast(blackList);
		
		//接受socket数据源
		JavaReceiverInputDStream<String> nameList = jsc.socketTextStream("node5", 9999);
		JavaPairDStream<String, String> pairNameList = nameList.mapToPair(new PairFunction<String, String, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(String s) throws Exception {
				return new Tuple2<String, String>(s.split(" ")[1], s);
			}
		});
		JavaDStream<String> transFormResult = pairNameList.transform(new Function<JavaPairRDD<String,String>, JavaRDD<String>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public JavaRDD<String> call(JavaPairRDD<String, String> nameRDD)
					throws Exception {
				JavaPairRDD<String, String> filter = 
						nameRDD.filter(new Function<Tuple2<String,String>, Boolean>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(Tuple2<String, String> v1)
							throws Exception {
						//得到广播变量
						List<String> blackList = broadcastList.value();
						
						return !blackList.contains(v1._1);
					}
				});
				return filter.map(new Function<Tuple2<String,String>, String>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public String call(Tuple2<String, String> v1)
							throws Exception {
						return v1._2;
					}
				});
			}
		});
		
		transFormResult.print();
		
		jsc.start();
		jsc.awaitTermination();
		jsc.stop();
	}
}