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

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPChannelProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.JCSMPTransportException;
import com.solacesystems.jcsmp.SessionEventArgs;
import com.solacesystems.jcsmp.SessionEventHandler;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageListener;
import com.solacesystems.jcsmp.XMLMessageProducer;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/** Direct Messaging sample to demonstrate initiating a request-reply flow.
 *  Makes use of the blocking convenience function Requestor.request();
 */
public class DirectRequestorAsync {

    private static final String SAMPLE_NAME = DirectRequestorAsync.class.getSimpleName();
    private static final String TOPIC_PREFIX = "solace/samples/";  // used as the topic "root"
    private static final String API = "JCSMP";
    private static final int REQUEST_TIMEOUT_MS = 3000;  // time to wait for a reply before timing out
    private static final int REQUEST_PERIOD_MS = 1000;
    private static final int CONCURRENT_REQUESTS = 4;

    private static Map<String, ScheduledFuture<?>> pendingRequests = new ConcurrentHashMap<>();

    private static volatile int msgSentCounter = 0;                   // num messages sent
    private static volatile boolean isShutdown = false;

    /** Main method. */
    public static void main(String... args) throws JCSMPException, IOException {
        if (args.length < 3) {  // Check command line arguments
            System.out.printf("Usage: %s <host:port> <message-vpn> <client-username> [password]%n%n", SAMPLE_NAME);
            System.exit(-1);
        }
        
        ScheduledExecutorService timeoutExecutor = Executors.newSingleThreadScheduledExecutor();
        ScheduledExecutorService simulatedWorker = Executors.newSingleThreadScheduledExecutor();

        Semaphore semaphore = new Semaphore(CONCURRENT_REQUESTS);

        System.out.println(API + " " + SAMPLE_NAME + " initializing...");

        final JCSMPProperties properties = new JCSMPProperties();
        properties.setProperty(JCSMPProperties.HOST, args[0]);          // host:port
        properties.setProperty(JCSMPProperties.VPN_NAME,  args[1]);     // message-vpn
        properties.setProperty(JCSMPProperties.USERNAME, args[2]);      // client-username
        if (args.length > 3) {
            properties.setProperty(JCSMPProperties.PASSWORD, args[3]);  // client-password
        }
        properties.setProperty(JCSMPProperties.REAPPLY_SUBSCRIPTIONS, true);  // re-subscribe after reconnect
        JCSMPChannelProperties channelProps = new JCSMPChannelProperties();
        channelProps.setReconnectRetries(20);      // recommended settings
        channelProps.setConnectRetriesPerHost(5);  // recommended settings
        // https://docs.solace.com/Solace-PubSub-Messaging-APIs/API-Developer-Guide/Configuring-Connection-T.htm
        properties.setProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES, channelProps);
        final JCSMPSession session;
        session = JCSMPFactory.onlyInstance().createSession(properties, null, new SessionEventHandler() {
            @Override
            public void handleEvent(SessionEventArgs event) {  // could be reconnecting, connection lost, etc.
                System.out.printf("### Received a Session event: %s%n", event);
            }
        });
        session.connect();  // connect to the broker

        // Simple anonymous inner-class for handling publishing events
        final XMLMessageProducer producer = session.getMessageProducer(new JCSMPStreamingPublishCorrelatingEventHandler() {
            // unused in Direct Messaging application, only for Guaranteed/Persistent publishing application
            @Override public void responseReceivedEx(Object key) {
            }

            // can be called for ACL violations, connection loss, and Persistent NACKs
            @Override
            public void handleErrorEx(Object key, JCSMPException cause, long timestamp) {
                System.out.printf("### Producer handleErrorEx() callback: %s%n", cause);
                if (cause instanceof JCSMPTransportException) {  // all reconnect attempts failed
                    isShutdown = true;  // let's quit; or, could initiate a new connection attempt
                }
            }
        });
    
        final XMLMessageConsumer consumer = session.getMessageConsumer(new XMLMessageListener() {
            @Override
            public void onException(JCSMPException cause) {
                System.out.printf("### Consumer onException() callback: %s%n", cause);
                if (cause instanceof JCSMPTransportException) {  // all reconnect attempts failed
                    isShutdown = true;  // let's quit; or, could initiate a new connection attempt
                }
            }

            @Override
            public void onReceive(BytesXMLMessage replyMsg) {
                ScheduledFuture<?> requestTimeout = pendingRequests.remove(replyMsg.getCorrelationId());
                if(requestTimeout != null) {
                    requestTimeout.cancel(false);
                    System.out.printf("vv Response Message Dump:%n%s%n", replyMsg.dump());
                    simulatedWorker.schedule(() -> { semaphore.release(); }, REQUEST_PERIOD_MS, TimeUnit.MILLISECONDS);
                } else {
                    System.out.printf("vv Received reponse for timed-out message.");
                }
            }
            
        });
        consumer.start();  // needed to receive the responses
        
        // topic should look like 'solace/samples/jcsmp/direct/request'
        final String requestTopicString = TOPIC_PREFIX + API.toLowerCase() + "/direct/request";
        final String replyTopicString = TOPIC_PREFIX + API.toLowerCase() + "/direct/reply";

        final Topic requestTopic = JCSMPFactory.onlyInstance().createTopic(requestTopicString);
        final Topic replyTopic = JCSMPFactory.onlyInstance().createTopic(replyTopicString);

        session.addSubscription(replyTopic);

        System.out.println(SAMPLE_NAME + " connected, and running. Press [ENTER] to quit.");
        while (System.in.available() == 0 && !isShutdown) {
            try {
                semaphore.acquire();

                try {
                    String correlationId = String.format("REQ%d", msgSentCounter);

                    TextMessage requestMsg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
                    requestMsg.setText(String.format("Hello, this is reqeust #%d", msgSentCounter));
                    requestMsg.setCorrelationId(correlationId);
                    requestMsg.setReplyTo(replyTopic);

                    System.out.printf(">> About to send request message #%d to topic '%s'...%n", msgSentCounter, requestTopicString);
                    producer.send(requestMsg, requestTopic);
                    
                    pendingRequests.put(correlationId, timeoutExecutor.schedule(() -> {
                        ScheduledFuture<?> requestTimeout = pendingRequests.remove(correlationId);
                        if(requestTimeout != null) {
                            System.out.println("Failed to receive a reply in " + REQUEST_TIMEOUT_MS + " msecs");
                            semaphore.release();
                        }
                    }, REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS));

                    
                } catch (JCSMPException e) {  // threw from send(), only thing that is throwing here, but keep trying (unless shutdown?)
                    System.out.printf("### Caught while trying to producer.send(): %s%n", e);
                    if (e instanceof JCSMPTransportException) {  // all reconnect attempts failed
                        isShutdown = true;  // let's quit; or, could initiate a new connection attempt
                    }
                }
                msgSentCounter++;  // add one
            } catch (InterruptedException e) {
                // Thread.sleep() interrupted... probably getting shut down
            }
        }
        isShutdown = true;
        timeoutExecutor.shutdown();
        session.closeSession();  // will also close producer and consumer objects
        System.out.println("Main thread quitting.");
    }
}
