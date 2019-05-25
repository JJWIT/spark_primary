package com.jjw;

import java.util.HashMap;  
import java.util.List;  
import java.util.Map;  
import java.util.Properties;  
  
import kafka.consumer.ConsumerConfig;  
import kafka.consumer.ConsumerIterator;  
import kafka.consumer.KafkaStream;  
import kafka.javaapi.consumer.ConsumerConnector;  
import kafka.serializer.StringDecoder;  
import kafka.utils.VerifiableProperties;

public class KafkaConsumer {
	  
    private final ConsumerConnector consumer;  
  
    private KafkaConsumer() {  
        Properties props = new Properties();  
          
        // zookeeper 配置  
        props.put("zookeeper.connect", "node3:2181,node4:2181,node5:2181");  
  
        // 消费者所在组  
        props.put("group.id", "MyGroup1");  
  
        // zk连接超时  
        props.put("zookeeper.session.timeout.ms", "4000");  
        props.put("zookeeper.sync.time.ms", "200");  
        props.put("auto.commit.interval.ms", "1000"); 
        /**
         * 此配置参数表示当此groupId下的消费者,在ZK中没有offset值时(比如新的groupId,或者是zk数据被清空),
         * consumer应该从哪个offset开始消费.largest表示接受接收最大的offset(即最新消息),
         * smallest表示最小offset,即从topic的开始位置消费所有消息.
         */
        props.put("auto.offset.reset", "smallest");  
          
        // 序列化类  
        props.put("serializer.class", "kafka.serializer.StringEncoder");  
  
        ConsumerConfig config = new ConsumerConfig(props);  
  
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);  
    }  
  
    void consume() {  
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();  
        topicCountMap.put("20170419", new Integer(1));  
  
        StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());  
        StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());  
  
//        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);  
//        KafkaStream<byte[], byte[]> stream = consumerMap.get("cmcccdr").get(0);  
//        ConsumerIterator<byte[], byte[]> it = stream.iterator();  
//          
//        while (it.hasNext()){  
//            System.out.println(new String(it.next().message()));  
//        }  
        Map<String, List<KafkaStream<String, String>>> consumerMap = consumer.createMessageStreams(topicCountMap,keyDecoder,valueDecoder);  
        KafkaStream<String, String> stream = consumerMap.get("20170419").get(0);  
        ConsumerIterator<String, String> it = stream.iterator();  
          
        while (it.hasNext()){  
            System.out.println(it.next().message());  
        }  
    }  
  
    public static void main(String[] args) {  
        new KafkaConsumer().consume();  
    }  
}  