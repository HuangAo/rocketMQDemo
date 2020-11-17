package com.huang.rocketmq.order;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Huang Ao
 * @date 2020/11/16 16:25
 * 发送顺序消息
 */
public class OrderProducer {
    public static void main(String[] args) throws Exception {
        //1.创建消息生产者producer，并制定生产者组名
        DefaultMQProducer producer = new DefaultMQProducer("group1");
        //2.指定Nameserver地址
        producer.setNamesrvAddr("192.168.127.201:9876");
        //3.启动producer
        producer.start();
        //构建消息集合
        //List<OrderStep> orderSteps = OrderStep.buildOrders();
        HashMap<String,Object> orderMessage = new HashMap<String, Object>();
        orderMessage.put("a","消息1-1");
        orderMessage.put("b","消息1-2");
        orderMessage.put("c","消息2-1");
        orderMessage.put("d","消息2-2");
        orderMessage.put("e","消息2-3");
        //发送消息
        for(Map.Entry<String,Object> entry: orderMessage.entrySet()){
            String body = entry.getValue().toString();
            Message message = new Message("OrderTopic","Order2",body.getBytes());
            SendResult result = producer.send(message, new MessageQueueSelector() {
                public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                    int messageQueueOrder = 0;
                    if(o.equals("a")||o.equals("b")){
                        messageQueueOrder = 0;
                    }else{
                        messageQueueOrder = 1;
                    }
                    return list.get(messageQueueOrder);
                }
            },entry.getKey());
            System.out.println("发送结果："+result);
        }

        producer.shutdown();
    }
}
