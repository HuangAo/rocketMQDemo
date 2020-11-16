package com.huang.rocketmq.base;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.concurrent.TimeUnit;

/**
 * @author Huang Ao
 * @date 2020/11/16 11:31
 * 发送同步消息
 */
public class SyncProducer {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        //1.创建消息生产者，设定生产者组名
        DefaultMQProducer producer = new DefaultMQProducer("group1");
        //2.指定NameServer地址
        producer.setNamesrvAddr("192.168.127.201:9876");
        //3.启动producer
        producer.start();


        for (int i=0;i<10;i++){
            //4.创建消息对象
            Message message = new Message("top1","tag1",("Hello,RocketMQ"+i).getBytes());
            //5.发送消息
            SendResult result = producer.send(message);
            System.out.println("发送状态:"+ result.getSendStatus());
            System.out.println("发送结果:"+ result);
            TimeUnit.SECONDS.sleep(1);
        }

        //6.关闭producer
        producer.shutdown();

    }
}
