package com.jjw.sparkStreaming;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;

import scala.Tuple2;

/**
 *
 *  Spark standalone or Mesos with cluster deploy mode only:
 *  在提交application的时候  添加 --supervise 选项  如果Driver挂掉 会自动启动一个Driver
 *
 */
public class SparkStreamingOnHDFS {
	public static void main(String[] args) {
		final SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkStreamingOnHDFS");
		
//		final String checkpointDirectory = "hdfs://node1:9000/spark/SparkStreaming/CheckPoint2017";
		final String checkpointDirectory = "./checkpoint";
		
		JavaStreamingContextFactory factory = new JavaStreamingContextFactory() {
			@Override
			public JavaStreamingContext create() {  
				return createContext(checkpointDirectory,conf);
			}
		};
		/**
		 * 获取JavaStreamingContext 先去指定的checkpoint目录中去恢复JavaStreamingContext
		 * 如果恢复不到，通过factory创建
		 */
		JavaStreamingContext jsc = JavaStreamingContext.getOrCreate(checkpointDirectory, factory);
		jsc.start();
		jsc.awaitTermination();
		jsc.close();
	}

//	@SuppressWarnings("deprecation")
	private static JavaStreamingContext createContext(String checkpointDirectory,SparkConf conf) {

		// If you do not see this printed, that means the StreamingContext has
		// been loaded
		// from the new checkpoint
		System.out.println("Creating new context");
		SparkConf sparkConf = conf;
		// Create the context with a 1 second batch size

		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));
//		ssc.sparkContext().setLogLevel("WARN");
		/**
		 *  checkpoint 保存：
		 *		1.配置信息
		 *		2.DStream操作逻辑
		 *		3.job的执行进度
		 *      4.offset
		 */
		ssc.checkpoint(checkpointDirectory);
		
		/**
		 * 监控的是HDFS上的一个目录，监控文件数量的变化     文件内容如果追加监控不到。
		 * 只监控文件夹下新增的文件，减少的文件时监控不到的，文件的内容有改动也监控不到。
		 */
//		JavaDStream<String> lines = ssc.textFileStream("hdfs://node1:9000/spark/sparkstreaming");
		JavaDStream<String> lines = ssc.textFileStream("./data");
		 
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(String s) {
				return Arrays.asList(s.split(" "));
			}
		});
		

		JavaPairDStream<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<String, Integer>(s.trim(), 1);
			}
		});

		JavaPairDStream<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});
		
		counts.print();
//		counts.filter(new Function<Tuple2<String,Integer>, Boolean>() {
//
//			/**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Boolean call(Tuple2<String, Integer> v1) throws Exception {
//				System.out.println("*************************");
//				return true;
//			}
//		}).print();
		return ssc;
	}
}
