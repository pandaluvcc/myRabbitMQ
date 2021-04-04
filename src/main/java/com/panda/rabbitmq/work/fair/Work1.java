package com.panda.rabbitmq.work.fair;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class Work1 {

    public static void main(String[] args) {
        // 1. 创建连接工厂
        ConnectionFactory factory = new ConnectionFactory();
        // 2. 设置连接属性
        factory.setVirtualHost("/");
        factory.setHost("127.0.0.1");
        factory.setPort(5672);
        factory.setUsername("admin");
        factory.setPassword("112233");

        Connection connection = null;
        Channel channel = null;

        try {
            // 3. 从连接工厂中获取连接
            connection = factory.newConnection("消费者-Work1");
            // 4. 从连接中获取通道
            channel = connection.createChannel();

            final Channel finalChannel = channel;
            finalChannel.basicQos(5);
            finalChannel.basicConsume("queue01", false, new DeliverCallback() {
                public void handle(String consumerTag, Delivery message) throws IOException {
                    System.out.println("Work1-收到消息：" + new String(message.getBody(), "UTF-8"));
                    try {
                        Thread.sleep(1000);
                        finalChannel.basicAck(message.getEnvelope().getDeliveryTag(), false);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }, new CancelCallback() {
                public void handle(String consumerTag) throws IOException {

                }
            });
            System.out.println("Work1-开始接收消息...");
            System.in.read();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 7. 关闭通道
            if (channel != null && channel.isOpen()) {
                try {
                    channel.close();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (TimeoutException e) {
                    e.printStackTrace();
                }
            }
            // 8. 关闭连接
            if (connection != null && connection.isOpen()) {
                try {
                    connection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
