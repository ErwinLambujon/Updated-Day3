package com.solace.samples.jcsmp.patterns;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishEventHandler;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageListener;
import com.solacesystems.jcsmp.XMLMessageProducer;
import com.solacesystems.jcsmp.*;

public class SecureSubscriber {
    private JCSMPSession session;
    private XMLMessageConsumer consumer;
    private Topic topic;

    public static void main(String... args) throws JCSMPException {
        String[] connectArgs = new String[] {
                "tcps://mr-connection-3b0hu8d008q.messaging.solace.cloud:55443", // Public Internet:
                "erwin-service", //Message VPN
                "solace-cloud-client", // username
                "fvn21bb2a31445bfar33a8sp8n" // password
        };

        SecureSubscriber subscriber = new SecureSubscriber();
        subscriber.run(connectArgs);
    }

    public void run(String... args) throws JCSMPException {
        JCSMPProperties properties = new JCSMPProperties();
        properties.setProperty(JCSMPProperties.HOST, args[0]);
        properties.setProperty(JCSMPProperties.VPN_NAME, args[1]);
        properties.setProperty(JCSMPProperties.USERNAME, args[2]);
        properties.setProperty(JCSMPProperties.PASSWORD, args[3]);

        properties.setProperty(JCSMPProperties.SSL_VALIDATE_CERTIFICATE, true);

        session = JCSMPFactory.onlyInstance().createSession(properties);
        session.connect();

        topic = JCSMPFactory.onlyInstance().createTopic("samples/hello");

        consumer = session.getMessageConsumer(new MessageHandler());
        session.addSubscription(topic);
        consumer.start();

        System.out.println("Connected securely. Subscribing to topic: " + topic.getName());
        System.out.println("Press [ENTER] to quit.");

        try {
            System.in.read();
        } catch (Exception ex) {
            // Ignored
        }

        consumer.close();
        session.closeSession();
    }

    private class MessageHandler implements XMLMessageListener {
        @Override
        public void onReceive(BytesXMLMessage message) {
            if (message instanceof TextMessage) {
                System.out.printf("\nReceived TextMessage: '%s'%n",
                        ((TextMessage) message).getText());
            }
            System.out.println("Message Details:");
            System.out.println("  Topic: " + message.getDestination());
            System.out.println("  Protocol Message ID: " + message.getMessageId());
        }

        @Override
        public void onException(JCSMPException e) {
            System.out.printf("Consumer received exception: %s%n", e);
        }
    }
}
