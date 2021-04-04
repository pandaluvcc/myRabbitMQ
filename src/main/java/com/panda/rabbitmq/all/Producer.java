package com.panda.rabbitmq.all;

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

            // 5. 准备消息内容
            String message = "Hello world...";

            // 6. 准备交换机
            String exchangeName = "direct-message-exchange";

            // 7. 定义路由key
            String routeKey = "course";

            // 8. 指定交换机类型
            String type = "direct";

            // 9. 声明交换机
            channel.exchangeDeclare(exchangeName, type, true);

            // 10. 声明队列
            channel.queueDeclare("queue01", true, false, false, null);
            channel.queueDeclare("queue02", true, false, false, null);
            channel.queueDeclare("queue03", true, false, false, null);

            // 11. 绑定队列和交换机
            channel.queueBind("queue01", exchangeName, "order");
            channel.queueBind("queue02", exchangeName, "order");
            channel.queueBind("queue03", exchangeName, "course");

            // 9. 发送消息至队列
            channel.basicPublish(exchangeName, routeKey, null, message.getBytes());
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
