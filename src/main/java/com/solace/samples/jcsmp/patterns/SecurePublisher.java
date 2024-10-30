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

public class SecurePublisher {
    private JCSMPSession session;
    private XMLMessageProducer producer;
    private Topic topic;

    public static void main(String... args) throws JCSMPException {
        String[] connectArgs = new String[] {
                "tcps://mr-connection-3b0hu8d008q.messaging.solace.cloud:55443",
                "erwin-service",
                "solace-cloud-client",
                "fvn21bb2a31445bfar33a8sp8n"
        };

        SecurePublisher publisher = new SecurePublisher();
        publisher.run(connectArgs);
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

        producer = session.getMessageProducer(new PublishEventHandler());
        topic = JCSMPFactory.onlyInstance().createTopic("samples/hello");

        System.out.println("Connected securely. Publishing messages to topic: " + topic.getName());

        TextMessage msg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
        for (int i = 0; i < 5; i++) {
            String messageText = "Message Number " + (i + 1);
            msg.setText(messageText);
            System.out.println("Publishing: " + messageText);
            producer.send(msg, topic);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // Ignored
            }
        }

        System.out.println("\nPress [ENTER] to quit.");
        try {
            System.in.read();
        } catch (Exception ex) {
        }

        session.closeSession();
    }

    private class PublishEventHandler implements JCSMPStreamingPublishEventHandler {
        @Override
        public void responseReceived(String messageID) {
            System.out.println("Message published successfully: " + messageID);
        }

        @Override
        public void handleError(String messageID, JCSMPException e, long timestamp) {
            System.out.printf("Error publishing message: %s@%s - %s%n", messageID, timestamp, e);
        }
    }
}