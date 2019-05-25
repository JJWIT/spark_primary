package com.jjw.sparkStreaming;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;
/**
 * 1、local的模拟线程数必须大于等于2 因为一条线程被receiver(接受数据的线程)占用，另外一个线程是job执行
 * 2、Durations时间的设置，就是我们能接受的延迟度，这个我们需要根据集群的资源情况以及监控每一个job的执行时间来调节出最佳时间。
 * 3、 创建JavaStreamingContext有两种方式 （sparkconf、sparkcontext）
 * 4、业务逻辑完成后，需要有一个output operator
 * 5、JavaStreamingContext.start()straming框架启动之后是不能在次添加业务逻辑
 * 6、JavaStreamingContext.stop()无参的stop方法会将sparkContext一同关闭，stop(false) ,默认为true，会一同关闭
 * 7、JavaStreamingContext.stop()停止之后是不能在调用start   
 */

/**
 * foreachRDD  算子注意：
 * 1.foreachRDD是DStream中output operator类算子
 * 2.foreachRDD可以遍历得到DStream中的RDD，可以在这个算子内对RDD使用RDD的Transformation类算子进行转化，但是一定要使用rdd的Action类算子触发执行。
 * 3.foreachRDD可以得到DStream中的RDD，在这个算子内，RDD算子外执行的代码是在Driver端执行的，RDD算子内的代码是在Executor中执行。
 *
 */
public class WordCountOnline {
	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		 
		/*SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("WordCountOnline");
		*//**
		 * 在创建streaminContext的时候 设置batch Interval
		 *//*
		JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
//		JavaSparkContext sc = new JavaSparkContext(conf);
//		JavaStreamingContext jsc = new JavaStreamingContext(sc,Durations.seconds(5));
//		JavaSparkContext sparkContext = jsc.sparkContext();
		
		JavaReceiverInputDStream<String> lines = jsc.socketTextStream("node5", 9999);
		
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			*//**
			 * 
			 *//*
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(String s) {
				return Arrays.asList(s.split(" "));
			}
		});

		JavaPairDStream<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
			*//**
			 * 
			 *//*
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<String, Integer>(s, 1);
			}
		});

		JavaPairDStream<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
			*//**
			 * 
			 *//*
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});
		 
		//outputoperator类的算子   
// 		counts.print();
// 		counts.foreachRDD(new VoidFunction<JavaPairRDD<String,Integer>>() {
//
//			*//**
//			 * 
//			 *//*
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public void call(JavaPairRDD<String, Integer> pairRDD) throws Exception {
//				
//				pairRDD.foreach(new VoidFunction<Tuple2<String,Integer>>() {
//
//					*//**
//					 * 
//					 *//*
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public void call(Tuple2<String, Integer> tuple)
//							throws Exception {
//						System.out.println("tuple ---- "+tuple );
//					}
//				});
//			}
//		});
 		jsc.start();
 		//等待spark程序被终止
 		jsc.awaitTermination();
 		jsc.stop(false);*/
	}
}
