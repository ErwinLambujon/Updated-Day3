/*
 * Copyright 2021-2022 Solace Corporation. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.solace.samples.jcsmp.patterns;

import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.JCSMPChannelProperties;
import com.solacesystems.jcsmp.JCSMPErrorResponseException;
import com.solacesystems.jcsmp.JCSMPErrorResponseSubcodeEx;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.JCSMPTransportException;
import com.solacesystems.jcsmp.SessionEventArgs;
import com.solacesystems.jcsmp.SessionEventHandler;
import com.solacesystems.jcsmp.XMLMessageProducer;

/**
 * A more performant sample that shows an application that publishes.
 */
import com.solacesystems.jcsmp.*;

import com.solacesystems.jcsmp.*;

public class DirectPublisher {
    private JCSMPSession session;
    private XMLMessageProducer producer;
    private Topic topic;

    public static void main(String... args) throws JCSMPException {
        if (args.length < 4) {
            System.out.println("Usage: DirectPublisher <host> <vpn> <username> <password>");
            System.exit(1);
        }

        DirectPublisher publisher = new DirectPublisher();
        publisher.run(args);
    }

    public void run(String... args) throws JCSMPException {
        JCSMPProperties properties = new JCSMPProperties();
        properties.setProperty(JCSMPProperties.HOST, args[0]);
        properties.setProperty(JCSMPProperties.VPN_NAME, args[1]);
        properties.setProperty(JCSMPProperties.USERNAME, args[2]);
        properties.setProperty(JCSMPProperties.PASSWORD, args[3]);

        session = JCSMPFactory.onlyInstance().createSession(properties);
        session.connect();

        producer = session.getMessageProducer(new PublishEventHandler());
        topic = JCSMPFactory.onlyInstance().createTopic("samples/hello");

        System.out.println("Connected. Publishing messages to topic: " + topic.getName());

        // Send 5 messages
        TextMessage msg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
        for (int i = 0; i < 5; i++) {
            String messageText = "Message Number " + (i + 1);
            msg.setText(messageText);
            System.out.println("Publishing: " + messageText);
            producer.send(msg, topic);
            try {
                Thread.sleep(1000);  // Wait 1 second between messages
            } catch (InterruptedException e) {
                // Ignored
            }
        }

        System.out.println("\nPress [ENTER] to quit.");
        try {
            System.in.read();
        } catch (Exception ex) {
            // Ignored
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