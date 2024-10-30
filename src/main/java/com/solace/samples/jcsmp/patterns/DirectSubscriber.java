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

import com.solacesystems.jcsmp.*;

public class DirectSubscriber {
    private JCSMPSession session;
    private XMLMessageConsumer consumer;
    private Topic topic;

    public static void main(String... args) throws JCSMPException {
        if (args.length < 4) {
            System.out.println("Usage: DirectSubscriber <host> <vpn> <username> <password>");
            System.exit(1);
        }

        DirectSubscriber subscriber = new DirectSubscriber();
        subscriber.run(args);
    }

    public void run(String... args) throws JCSMPException {
        JCSMPProperties properties = new JCSMPProperties();
        properties.setProperty(JCSMPProperties.HOST, args[0]);
        properties.setProperty(JCSMPProperties.VPN_NAME, args[1]);
        properties.setProperty(JCSMPProperties.USERNAME, args[2]);
        properties.setProperty(JCSMPProperties.PASSWORD, args[3]);

        session = JCSMPFactory.onlyInstance().createSession(properties);
        session.connect();

        topic = JCSMPFactory.onlyInstance().createTopic("samples/hello");

        // Create consumer and subscribe to topic
        consumer = session.getMessageConsumer(new MessageHandler());
        session.addSubscription(topic);
        consumer.start();

        System.out.println("Connected. Subscribing to topic: " + topic.getName());
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
            } else {
                System.out.println("\nReceived message of type: " +
                        message.getClass().getSimpleName());
            }

            // Print additional message details
            System.out.println("Message Details:");
            System.out.println("  Topic: " + message.getDestination());
            System.out.println("  Priority: " + message.getPriority());
            System.out.println("  Class: " + message.getClass().getSimpleName());
        }

        @Override
        public void onException(JCSMPException e) {
            System.out.printf("Consumer received exception: %s%n", e);
        }
    }
}
