package com.huang.rocketmq.base;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.TimeUnit;

/**
 * @author Huang Ao
 * @date 2020/11/16 14:32
 * 发送单向消息
 */
public class OneWayProducer {
    public static void main(String[] args) throws Exception {
        //1.创建消息生产者，设定生产者组名
        DefaultMQProducer producer = new DefaultMQProducer("group1");
        //2.指定NameServer地址
        producer.setNamesrvAddr("192.168.127.201:9876");
        //3.启动producer
        producer.start();

        for (int i=0;i<10;i++){
            //4.创建消息对象
            Message message = new Message("top1","tag3",("Hello,RocketMQ,OneWayMessage"+i).getBytes());
            //5.发送单向消息
            producer.sendOneway(message);
            TimeUnit.SECONDS.sleep(1);
        }

        //6.关闭producer
        producer.shutdown();
    }
}
