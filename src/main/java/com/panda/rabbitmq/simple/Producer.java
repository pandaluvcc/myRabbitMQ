package com.panda.rabbitmq.simple;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 简单模式 - 生产者
 * @author Charles
 */
public class Producer {

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
            connection = factory.newConnection("生产者");
            // 3. 通过连接获取通道 (Channel)
            channel = connection.createChannel();
            // 4. 通过通道创建交换机，声明队列、绑定关系、路由 key、发送消息和接收消息
            String queueName = "queue1";
            // 队列名、是否持久化、是否排他性、是否自动删除、声明队列时是否携带额外参数
            channel.queueDeclare(queueName, false, false, false, null);
            // 5. 准备消息内容
            String message = "Hello world...";
            // 6. 发送消息至队列
            channel.basicPublish("", queueName, null, message.getBytes());
            System.out.println("消息发送成功...");
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
