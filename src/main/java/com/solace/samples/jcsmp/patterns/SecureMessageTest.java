package com.solace.samples.jcsmp.patterns;

import com.solacesystems.jcsmp.*;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class SecureMessageTest {
    private static final String HOST = "tcps://mr-connection-3b0hu8d008q.messaging.solace.cloud:55443";
    private static final String VPN = "erwin-service";
    private static final String USERNAME = "solace-cloud-client";
    private static final String PASSWORD = "fvn21bb2a31445bfar33a8sp8n";
    private static final String TOPIC_NAME = "samples/hello";

    private static final CountDownLatch messageLatch = new CountDownLatch(1);
    private static String receivedMessage = null;

    public static void main(String... args) throws Exception {
        // First, verify TLS connection is available
        verifyTLSConnection();

        // Start subscriber first
        Thread subscriberThread = new Thread(() -> {
            try {
                runSubscriber();
            } catch (JCSMPException e) {
                System.err.println("Subscriber error: " + e.getMessage());
            }
        });
        subscriberThread.start();

        // Wait for subscriber to initialize
        Thread.sleep(2000);

        // Then run publisher
        runPublisher();

        // Wait for message to be received
        boolean messageReceived = messageLatch.await(10, TimeUnit.SECONDS);
        if (messageReceived) {
            System.out.println("\n✅ Secure messaging test completed successfully!");
            System.out.println("Message was securely transmitted and received over TLS");
        } else {
            System.out.println("\n❌ Test failed: Message was not received within timeout period");
        }
    }

    private static void verifyTLSConnection() {
        try {
            // Extract host and port from URL
            String host = HOST.replace("tcps://", "").split(":")[0];
            int port = Integer.parseInt(HOST.split(":")[2]);

            // Create SSL Socket
            SSLSocketFactory factory = (SSLSocketFactory) SSLSocketFactory.getDefault();
            SSLSocket socket = (SSLSocket) factory.createSocket(host, port);

            // Get SSL Session details
            socket.startHandshake();
            SSLContext context = SSLContext.getDefault();

            System.out.println("\n🔒 TLS Connection Details:");
            System.out.println("Protocol: " + socket.getSession().getProtocol());
            System.out.println("Cipher Suite: " + socket.getSession().getCipherSuite());
            System.out.println("Connected to: " + socket.getSession().getPeerHost());

            socket.close();
        } catch (Exception e) {
            System.err.println("Failed to verify TLS connection: " + e.getMessage());
            System.exit(1);
        }
    }

    private static void runSubscriber() throws JCSMPException {
        // Create and set up properties
        JCSMPProperties properties = new JCSMPProperties();
        properties.setProperty(JCSMPProperties.HOST, HOST);
        properties.setProperty(JCSMPProperties.VPN_NAME, VPN);
        properties.setProperty(JCSMPProperties.USERNAME, USERNAME);
        properties.setProperty(JCSMPProperties.PASSWORD, PASSWORD);
        properties.setProperty(JCSMPProperties.SSL_VALIDATE_CERTIFICATE, true);

        // Create session and connect
        JCSMPSession session = JCSMPFactory.onlyInstance().createSession(properties);
        session.connect();

        // Create topic and subscribe
        Topic topic = JCSMPFactory.onlyInstance().createTopic(TOPIC_NAME);
        XMLMessageConsumer consumer = session.getMessageConsumer(new XMLMessageListener() {
            @Override
            public void onReceive(BytesXMLMessage message) {
                if (message instanceof TextMessage) {
                    receivedMessage = ((TextMessage) message).getText();
                    System.out.println("\n📥 Received secure message: " + receivedMessage);
                    messageLatch.countDown();
                }
            }

            @Override
            public void onException(JCSMPException e) {
                System.err.println("Consumer received exception: " + e);
            }
        });

        session.addSubscription(topic);
        consumer.start();
        System.out.println("🎧 Secure subscriber started on topic: " + TOPIC_NAME);
    }

    private static void runPublisher() throws JCSMPException {
        // Create and set up properties
        JCSMPProperties properties = new JCSMPProperties();
        properties.setProperty(JCSMPProperties.HOST, HOST);
        properties.setProperty(JCSMPProperties.VPN_NAME, VPN);
        properties.setProperty(JCSMPProperties.USERNAME, USERNAME);
        properties.setProperty(JCSMPProperties.PASSWORD, PASSWORD);
        properties.setProperty(JCSMPProperties.SSL_VALIDATE_CERTIFICATE, true);

        // Create session and connect
        JCSMPSession session = JCSMPFactory.onlyInstance().createSession(properties);
        session.connect();

        // Create producer
        XMLMessageProducer producer = session.getMessageProducer(new JCSMPStreamingPublishEventHandler() {
            @Override
            public void responseReceived(String messageID) {
                System.out.println("✅ Message published successfully with ID: " + messageID);
            }

            @Override
            public void handleError(String messageID, JCSMPException e, long timestamp) {
                System.err.println("❌ Error publishing message: " + messageID + " - " + e);
            }
        });

        // Create and send test message
        Topic topic = JCSMPFactory.onlyInstance().createTopic(TOPIC_NAME);
        TextMessage msg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
        msg.setText("Secure Test Message " + System.currentTimeMillis());

        System.out.println("\n📤 Publishing secure message to topic: " + TOPIC_NAME);
        producer.send(msg, topic);
    }
}