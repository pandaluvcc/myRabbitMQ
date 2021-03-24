package com.panda.rabbitmq.simple;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 简单模式 - 消费者
 * @author Charles
 */
public class Consumer {
    public static void main(String[] args) {
        // 1. 创建连接
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("127.0.0.1");
        factory.setPort(5672);
        factory.setUsername("admin");
        factory.setPassword("admin");
        factory.setVirtualHost("/");

        Connection connection = null;
        Channel channel = null;
        try {
            // 2. 创建连接 Connection
            connection = factory.newConnection("消费者");
            // 3. 通过连接获取通道 (Channel)
            channel = connection.createChannel();
            // 4. 通过通道创建交换机，声明队列、绑定关系、路由 key、发送消息和接收消息
            String queName = "queue1";
            channel.basicConsume(queName, true, new DeliverCallback() {
                public void handle(String consumerTag, Delivery message) throws IOException {
                    System.out.println("获取到 queue1 队列的消息：" + new String(message.getBody(), "UTF-8"));
                }
            }, new CancelCallback() {
                public void handle(String consumerTag) throws IOException {
                    System.out.println("获取 queue1 队列的消息失败");
                }
            });
            System.out.println("接收消息...");
            System.in.read();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
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
