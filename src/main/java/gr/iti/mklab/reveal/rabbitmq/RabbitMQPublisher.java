package gr.iti.mklab.reveal.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;

/**
 * A basic RabbitMQ Publisher
 *
 * @author kandreadou
 */
public class RabbitMQPublisher {

    private Channel channel;
    private Connection connection;
    private String exchangeName;

    public RabbitMQPublisher(String host, String collection) {
        try {
            this.exchangeName = collection;
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(host);
            // Do we need the methods below? port probably 5672, user "guest"
            // setPort, setUsername, setPassword
            connection = factory.newConnection();
            channel = connection.createChannel();
            channel.exchangeDeclare(collection, "topic");
        } catch (IOException ioe) {
            System.out.println(ioe);
        }
    }

    public void publish(String message) {
        try {
            System.out.println("RABBITMQ publish: "+message);
            channel.basicPublish(exchangeName, "test-rooting", null, message.getBytes());
        } catch (IOException ioe) {
            System.out.println(ioe);
        }
    }

    public void close() {
        try {
            channel.close();
            connection.close();
        } catch (Exception e) {
            //ignore
        }
    }
}
