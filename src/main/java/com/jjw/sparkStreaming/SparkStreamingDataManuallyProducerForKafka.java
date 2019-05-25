package com.jjw.sparkStreaming;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;
/**
 * 向kafka中生产数据
 * @author root
 *
 */
public class SparkStreamingDataManuallyProducerForKafka extends Thread{
	
    static String[] channelNames = new  String[]{
    	"Spark","Scala","Kafka","Flink","Hadoop","Storm",
    	"Hive","Impala","HBase","ML"
    };

    static String[] actionNames = new String[]{"View", "Register"};

    private String topic; //发送给Kafka的数据,topic
    private Producer<String, String> producerForKafka;
    
    private static String dateToday;
    private static Random random;

    public SparkStreamingDataManuallyProducerForKafka(String topic){
    	dateToday = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
        this.topic = topic;
        random = new Random();
        Properties properties = new Properties();
        properties.put("metadata.broker.list","node1:9092,node2:9092,node3:9092");
        //发送消息key的编码格式
        properties.put("key.serializer.class",  StringEncoder.class.getName());
        //发送消息value的编码格式
        properties.put("serializer.class",  StringEncoder.class.getName());
        producerForKafka = new Producer<String, String>(new ProducerConfig(properties)) ;
    }
    
    
    @Override
    public void run() {
    	int counter = 0;
    	while(true){
    		counter++;
			String userLog = userlogs();
//			System.out.println("product:"+userLog+"   ");
//			producerForKafka.send(new KeyedMessage<String, String>(topic,userLog));
			producerForKafka.send(new KeyedMessage<String, String>(topic,userLog));
			//每两条数据暂停2秒
			if(0 == counter%2){
				counter = 0;
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			}
    	}
    

    public static void main( String[] args ){
    	
    	new SparkStreamingDataManuallyProducerForKafka("t0404").start();
    	
    }

	
    //生成随机数据
	private static String userlogs() {
		
		StringBuffer userLogBuffer = new StringBuffer("");
		int[] unregisteredUsers = new int[]{1, 2, 3, 4, 5, 6, 7, 8};
		long timestamp = new Date().getTime();
		Long userID = 0L;
		long pageID = 0L;
			
		//随机生成的用户ID 
		if(unregisteredUsers[random.nextInt(8)] == 1) {
			userID = null;
		} else {
			userID = (long) random.nextInt(2000);
		}
		
		
		//随机生成的页面ID
		pageID =  random.nextInt(2000);
		
		//随机生成Channel
		String channel = channelNames[random.nextInt(10)];
		
		//随机生成action行为
		String action = actionNames[random.nextInt(2)];
		
		
		userLogBuffer.append(dateToday)
					.append("\t")
					.append(timestamp)
					.append("\t")
					.append(userID)
					.append("\t")
					.append(pageID)
					.append("\t")
					.append(channel)
					.append("\t")
					.append(action);
//						.append("\n");	
		
		System.out.println(userLogBuffer.toString());
		return userLogBuffer.toString();
	}

}
