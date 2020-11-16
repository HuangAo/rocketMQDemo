package com.huang.rocketmq.base;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.TimeUnit;

/**
 * @author Huang Ao
 * @date 2020/11/16 14:10
 * 发送异步消息
 */
public class AsyncProducer {
    public static void main(String[] args) throws Exception {
        //1.创建消息生产者，设定生产者组名
        DefaultMQProducer producer = new DefaultMQProducer("group1");
        //2.指定NameServer地址
        producer.setNamesrvAddr("192.168.127.201:9876");
        //3.启动producer
        producer.start();


        for (int i=0;i<10;i++){
            //4.创建消息对象
            Message message = new Message("top1","tag2",("Hello,RocketMQ"+i).getBytes());
            //5.发送异步消息
            producer.send(message, new SendCallback() {
                public void onSuccess(SendResult sendResult) {
                    System.out.println("发送成功："+ sendResult);
                }

                public void onException(Throwable throwable) {
                    System.out.println("发送异常:"+ throwable);
                }
            });

            TimeUnit.SECONDS.sleep(1);
        }

        //6.关闭producer
        producer.shutdown();
    }
}
