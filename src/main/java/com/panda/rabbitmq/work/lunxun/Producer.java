package com.panda.rabbitmq.work.lunxun;

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
        factory.setPassword("112233");
        factory.setVirtualHost("/");

        Connection connection = null;
        Channel channel = null;
        try {
            // 2. 创建连接 Connection
            connection = factory.newConnection("生产者");
            // 3. 通过连接获取通道 (Channel)
            channel = connection.createChannel();

            // 4. 准备消息内容
            for (int i = 1; i <= 20; i++) {
                String message = "Hello world..." + i;
                // 5. 发送消息至队列  不显示指定交换机时，routingkey 为队列名
                channel.basicPublish("", "queue01", null, message.getBytes());
            }
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
