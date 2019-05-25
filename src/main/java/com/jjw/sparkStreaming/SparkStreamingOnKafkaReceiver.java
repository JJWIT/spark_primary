package com.jjw.sparkStreaming;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;
/**
 * receiver 模式并行度是由blockInterval决定的
 * @author root
 *
 */
public class SparkStreamingOnKafkaReceiver {
 
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SparkStreamingOnKafkaReceiver")
				.setMaster("local[2]");
		//开启预写日志 WAL机制
		conf.set("spark.streaming.receiver.writeAheadLog.enable","true");
		
		JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
  		jsc.checkpoint("./receivedata");
		
		Map<String, Integer> topicConsumerConcurrency = new HashMap<String, Integer>();
		/**
		 * 设置读取的topic和接受数据的线程数
		 */
		topicConsumerConcurrency.put("t0404", 1);
		
		/**
		 * 第一个参数是StreamingContext
		 * 第二个参数是ZooKeeper集群信息（接受Kafka数据的时候会从Zookeeper中获得Offset等元数据信息）
		 * 第三个参数是Consumer Group 消费者组
		 * 第四个参数是消费的Topic以及并发读取Topic中Partition的线程数
		 * 
		 * 注意：
		 * KafkaUtils.createStream 使用五个参数的方法，设置receiver的存储级别
		 */
		JavaPairReceiverInputDStream<String,String> lines = KafkaUtils.createStream(
				jsc,
				"node3:2181,node4:2181,node5:2181",
				"MyFirstConsumerGroup", 
				topicConsumerConcurrency);
		
//		JavaPairReceiverInputDStream<String,String> lines = KafkaUtils.createStream(
//				jsc,
//				"node3:2181,node4:2181,node5:2181",
//				"MyFirstConsumerGroup", 
//				topicConsumerConcurrency/*,
//				StorageLevel.MEMORY_AND_DISK()*/);
		
		
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<Tuple2<String,String>, String>() { 

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Iterable<String> call(Tuple2<String,String> tuple) throws Exception {
				return Arrays.asList(tuple._2.split("\t"));
			}
		});
		
		  
		JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}
		});
		
		  
		JavaPairDStream<String, Integer> wordsCount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() { 
			//对相同的Key，进行Value的累计（包括Local和Reducer级别同时Reduce）
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		
		 
		wordsCount.print(100);
		
		jsc.start();
		jsc.awaitTermination();
		jsc.close();
	}

}
