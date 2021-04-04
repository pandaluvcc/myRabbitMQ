package com.panda.rabbitmq.routing;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 简单模式 - 消费者
 *
 * @author Charles
 */
public class Consumer {

    public static void main(String[] args) {
        new Thread(runnable, "fanout-queue01").start();
        new Thread(runnable, "fanout-queue02").start();
    }

    private static Runnable runnable = new Runnable() {
        public void run() {
            // 1. 创建连接
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("127.0.0.1");
            factory.setPort(5672);
            factory.setUsername("admin");
            factory.setPassword("112233");
            factory.setVirtualHost("/");

            final String queueName = Thread.currentThread().getName();
            Connection connection = null;
            Channel channel = null;
            try {
                // 2. 创建连接 Connection
                connection = factory.newConnection("消费者");
                // 3. 通过连接获取通道 (Channel)
                channel = connection.createChannel();

                Channel finalChannel = channel;

                // 4. 通过通道创建交换机，声明队列、绑定关系、路由 key、发送消息和接收消息
                finalChannel.basicConsume(queueName, true, new DeliverCallback() {
                    public void handle(String consumerTag, Delivery message) throws IOException {
                        System.out.println(message.getEnvelope().getDeliveryTag());
                        System.out.println(queueName + ": 收到消息是: " + new String(message.getBody(), "UTF-8"));
                    }
                }, new CancelCallback() {
                    public void handle(String consumerTag) throws IOException {

                    }
                });
                System.out.println("开始接收消息...");
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
    };
}
