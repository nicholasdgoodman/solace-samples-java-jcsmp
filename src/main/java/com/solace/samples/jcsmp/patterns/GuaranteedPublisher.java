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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.JCSMPChannelProperties;
import com.solacesystems.jcsmp.JCSMPErrorResponseException;
import com.solacesystems.jcsmp.JCSMPErrorResponseSubcodeEx;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProducerEventHandler;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.JCSMPTransportException;
import com.solacesystems.jcsmp.Pair;
import com.solacesystems.jcsmp.ProducerEventArgs;
import com.solacesystems.jcsmp.ProducerFlowProperties;
import com.solacesystems.jcsmp.SDTException;
import com.solacesystems.jcsmp.SDTMap;
import com.solacesystems.jcsmp.SessionEventArgs;
import com.solacesystems.jcsmp.SessionEventHandler;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessage;
import com.solacesystems.jcsmp.XMLMessageProducer;
import com.solacesystems.jcsmp.transaction.RollbackException;
import com.solacesystems.jcsmp.transaction.TransactedSession;
import com.solacesystems.jcsmp.transaction.TransactionResultUnknownException;

public class GuaranteedPublisher {
    private static final String SAMPLE_NAME = GuaranteedPublisher.class.getSimpleName();
    static final String TOPIC_PREFIX = "solace/samples";  // used as the topic "root"
    private static final String API = "JCSMP";
    private static final int APPROX_MSG_RATE_PER_SEC = 50;
    private static final int MESSAGES_PER_BURST = 10;
    private static final int PAYLOAD_SIZE = 512;
    
    // remember to add log4j2.xml to your classpath
    private static final Logger logger = LogManager.getLogger();  // log4j2, but could also use SLF4J, JCL, etc.

    private static volatile int msgSentCounter = 0;                   // num messages sent

    /** Main. */
    public static void main(String... args) throws JCSMPException, IOException, InterruptedException {
        if (args.length < 4) {  // Check command line arguments
            printUsageAndExit();
            return;
        }
        System.out.println(API + " " + SAMPLE_NAME + " initializing...");

        String mode = args[0];
        String host = args[1];
        String vpnName = args[2];
        String userName = args[3];
        String password = (args.length > 4) ? args[4] : "";

        Publisher publisher;
        switch(mode.toLowerCase()) {
            case "windowed":
                publisher = new WindowedPublisher(host, vpnName, userName, password);
                break;
            case "onebyone":
                publisher = new OneByOneMessagePublisher(host, vpnName, userName, password);
                break;
            case "blocking":
                publisher = new BlockingMessagePublisher(host, vpnName, userName, password);
                break;
            case "transacted":
                publisher = new TransactedPublisher(host, vpnName, userName, password);
                break;
            default:
                printUsageAndExit();
                return;
        }

        publisher.connect();
        System.out.println(API + " " + SAMPLE_NAME + " connected, and running. Press [ENTER] to quit.");
        
        ScheduledExecutorService statsPrintingThread = Executors.newSingleThreadScheduledExecutor();
        statsPrintingThread.scheduleAtFixedRate(() -> {
            System.out.printf("%s %s Published msgs/s: %,d%n", API, SAMPLE_NAME, msgSentCounter);  // simple way of calculating message rates
            msgSentCounter = 0;
        }, 1, 1, TimeUnit.SECONDS);
        
        byte[] payload = new byte[PAYLOAD_SIZE];  // preallocate
        
        String topicStart = String.join("/", TOPIC_PREFIX, API.toLowerCase(), "pers/pub");
        System.out.printf("Publishing to topic '%s/...', please ensure queue has matching subscription.%n", topicStart); 
        
        ScheduledExecutorService messageSendThread = Executors.newSingleThreadScheduledExecutor();
        messageSendThread.scheduleAtFixedRate(() -> {
            try {
                // each loop, change the payload as an example
                char chosenCharacter = (char)(Math.round(msgSentCounter % 26) + 65);  // choose a "random" letter [A-Z]
                Arrays.fill(payload,(byte)chosenCharacter);  // fill the payload completely with that char

                String topicString = String.join("/", topicStart, String.valueOf(chosenCharacter));
                // NOTE: publishing to topic, so make sure GuaranteedSubscriber queue is subscribed to same topic,
                //       or enable "Reject Message to Sender on No Subscription Match" the client-profile
                for(int n = 0; n < MESSAGES_PER_BURST; n++) {
                    publisher.sendMessage(payload, topicString);
                    msgSentCounter++;
                }
            } catch (JCSMPException e) {  // threw from send(), only thing that is throwing here, but keep trying (unless shutdown?)
                logger.warn("### Caught while trying to producer.send()",e);
                if (e instanceof JCSMPTransportException) {  // all reconnect attempts failed
                    messageSendThread.shutdown();
                }
            }
        }, 0, 1000 / (APPROX_MSG_RATE_PER_SEC / MESSAGES_PER_BURST), TimeUnit.MILLISECONDS);

        System.in.read();
        messageSendThread.shutdown();
        statsPrintingThread.shutdown();  // stop printing stats
        Thread.sleep(1500);  // give time for Final ACKs to arrive from the broker

        publisher.close();
        System.out.println("Main thread quitting.");
    }

    private static void printUsageAndExit() {
        System.out.printf("Usage: %s [Windowed|OneByOne|Blocking|Transacted] <host:port> <message-vpn> <client-username> [password]%n%n", SAMPLE_NAME);
        System.exit(-1);
    }

    private static interface Publisher {
        public void connect() throws JCSMPException;
        public void sendMessage(byte[] payload, String topicString) throws JCSMPException;
        public void close();
    }
    private static class WindowedPublisher implements Publisher {
        private static final int PUBLISH_WINDOW_SIZE = 20;
        private static final int CHANNEL_RECONNECT_RETRIES = 20;
        private static final int CHANNEL_CONNECT_RETRIES_PER_HOST = 5;

        private JCSMPProperties sessionProps;
        private JCSMPSession session;
        private XMLMessageProducer producer;
        private int sequenceNumber = 0;

        private WindowedPublisher(String host, String vpnName, String userName, String password) {
            this.sessionProps = new JCSMPProperties();
            sessionProps.setProperty(JCSMPProperties.HOST, host);             // host:port
            sessionProps.setProperty(JCSMPProperties.VPN_NAME,  vpnName);     // message-vpn
            sessionProps.setProperty(JCSMPProperties.USERNAME, userName);     // client-username
            if (password.compareTo("") != 0) {
                sessionProps.setProperty(JCSMPProperties.PASSWORD, password); // client-password
            }
            sessionProps.setProperty(JCSMPProperties.PUB_ACK_WINDOW_SIZE, PUBLISH_WINDOW_SIZE);

            JCSMPChannelProperties channelProps = new JCSMPChannelProperties();
            channelProps.setReconnectRetries(CHANNEL_RECONNECT_RETRIES);      // recommended settings
            channelProps.setConnectRetriesPerHost(CHANNEL_CONNECT_RETRIES_PER_HOST);  // recommended settings
            sessionProps.setProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES, channelProps);
        }

        public void connect() throws JCSMPException {
            this.session = JCSMPFactory.onlyInstance().createSession(this.sessionProps, null, new SessionEventHandler() {
                @Override
                public void handleEvent(SessionEventArgs event) {  // could be reconnecting, connection lost, etc.
                    logger.info("### Received a Session event: " + event);
                }
            });
            this.session.connect();
            this.producer = this.createMessageProducer();
        }

        public void sendMessage(byte[] payload, String topicString) throws JCSMPException {
            XMLMessage message = createMessage(payload);
            Topic topic = JCSMPFactory.onlyInstance().createTopic(topicString);
            
            logger.info("Message [{}] Sending...", message.getApplicationMessageId());

            message.setCorrelationKey(new Pair<XMLMessage,Topic>(message, topic));  // used for ACK/NACK correlation locally within the API
            this.producer.send(message, topic);
            
            logger.info("Message [{}] Sent", message.getApplicationMessageId());
            sequenceNumber++;
        }
        
        public void close() {
            this.session.closeSession();
        }

        private XMLMessageProducer createMessageProducer() throws JCSMPException {
            return this.session.getMessageProducer(
                new JCSMPStreamingPublishCorrelatingEventHandler() {
                    @Override
                    public void responseReceivedEx(Object key) {
                        if (key != null) {
                            @SuppressWarnings("unchecked")
                            Pair<XMLMessage,Topic> messageAndTopic = (Pair<XMLMessage,Topic>)key;
                            logger.info("Message [{}] Acknowledged", messageAndTopic.getFirst().getApplicationMessageId());
                        }
                    }

                    // Here's where we handle failed publish attempts, there are many options on what to do next:
                    // - send the message again
                    // - send it somewhere else (e.g., error handling queue)
                    // - log and continue
                    // - pause and retry (backoff) - maybe set a flag to slow down the publisher
                    @Override
                    public void handleErrorEx(Object key, JCSMPException cause, long timestamp) {
                        if (key != null) {
                            @SuppressWarnings("unchecked")
                            Pair<XMLMessage,Topic> messageAndTopic = (Pair<XMLMessage,Topic>)key;
                            logger.info("Message [{}] Rejected", messageAndTopic.getFirst().getApplicationMessageId());  // e.g.: log and continue
                        } else {  // not a NACK, but some other error (ACL violation, connection loss, message too big, ...)
                            logger.warn("### Producer handleErrorEx() callback: %s%n", cause);
                            if (cause instanceof JCSMPTransportException) {  // all reconnect attempts failed
                                session.closeSession();
                            } else if (cause instanceof JCSMPErrorResponseException) {  // might have some extra info
                                JCSMPErrorResponseException e = (JCSMPErrorResponseException)cause;
                                logger.warn("Specifics: " + JCSMPErrorResponseSubcodeEx.getSubcodeAsString(e.getSubcodeEx()) + ": " + e.getResponsePhrase());
                            }
                        }
                    }
                },
                new JCSMPProducerEventHandler() {
                    @Override
                    public void handleEvent(ProducerEventArgs event) {
                        // as of JCSMP v10.10, this event only occurs when republishing unACKed messages
                        // on an unknown flow (DR failover)
                        logger.info("*** Received a producer event: " + event);
                    }
                });
        }
        private XMLMessage createMessage(byte[] payload) throws SDTException {
            BytesMessage message = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
            message.setData(payload);
            message.setDeliveryMode(DeliveryMode.PERSISTENT);  // required for Guaranteed
            message.setApplicationMessageId(String.format("%08d", sequenceNumber));  // as an example
            message.setSequenceNumber(sequenceNumber);

            // as another example, let's define a user property!
            SDTMap map = JCSMPFactory.onlyInstance().createMap();
            map.putString("sample", API + "_" + SAMPLE_NAME);
            message.setProperties(map);
            return message;
        }
    }
    private static class OneByOneMessagePublisher implements Publisher {
        private static final int PUBLISH_WINDOW_SIZE = 1;
        private static final int CHANNEL_RECONNECT_RETRIES = 20;
        private static final int CHANNEL_CONNECT_RETRIES_PER_HOST = 5;

        private JCSMPProperties sessionProps;
        private JCSMPSession session;
        private XMLMessageProducer producer;
        private int sequenceNumber = 0;

        public OneByOneMessagePublisher(String host, String vpnName, String userName, String password) {
            this.sessionProps = new JCSMPProperties();
            sessionProps.setProperty(JCSMPProperties.HOST, host);             // host:port
            sessionProps.setProperty(JCSMPProperties.VPN_NAME,  vpnName);     // message-vpn
            sessionProps.setProperty(JCSMPProperties.USERNAME, userName);     // client-username
            if (password.compareTo("") != 0) {
                sessionProps.setProperty(JCSMPProperties.PASSWORD, password); // client-password
            }
            sessionProps.setProperty(JCSMPProperties.PUB_ACK_WINDOW_SIZE, PUBLISH_WINDOW_SIZE);

            JCSMPChannelProperties channelProps = new JCSMPChannelProperties();
            channelProps.setReconnectRetries(CHANNEL_RECONNECT_RETRIES);      // recommended settings
            channelProps.setConnectRetriesPerHost(CHANNEL_CONNECT_RETRIES_PER_HOST);  // recommended settings
            sessionProps.setProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES, channelProps);
        }

        public void connect() throws JCSMPException {
            this.session = JCSMPFactory.onlyInstance().createSession(this.sessionProps, null, new SessionEventHandler() {
                @Override
                public void handleEvent(SessionEventArgs event) {  // could be reconnecting, connection lost, etc.
                    logger.info("### Received a Session event: " + event);
                }
            });
            this.session.connect();
            this.producer = this.createMessageProducer();
        }

        public void sendMessage(byte[] payload, String topicString) throws JCSMPException {
            XMLMessage message = createMessage(payload);
            Topic topic = JCSMPFactory.onlyInstance().createTopic(topicString);
            
            logger.info("Message [{}] Sending...", message.getApplicationMessageId());
            
            message.setCorrelationKey(new Pair<XMLMessage,Topic>(message, topic));  // used for ACK/NACK correlation locally within the API
            this.producer.send(message, topic);
            
            logger.info("Message [{}] Sent", message.getApplicationMessageId());
            sequenceNumber++;
        }
        
        public void close() {
            this.session.closeSession();
        }

        private XMLMessageProducer createMessageProducer() throws JCSMPException {
            return this.session.getMessageProducer(
                new JCSMPStreamingPublishCorrelatingEventHandler() {
                    @Override
                    public void responseReceivedEx(Object key) {
                        if (key != null) {
                            @SuppressWarnings("unchecked")
                            Pair<XMLMessage,Topic> messageAndTopic = (Pair<XMLMessage,Topic>)key;
                            logger.info("Message [{}] Acknowledged", messageAndTopic.getFirst().getApplicationMessageId());
                        }
                    }

                    // Here's where we handle failed publish attempts, there are many options on what to do next:
                    // - send the message again
                    // - send it somewhere else (e.g., error handling queue)
                    // - log and continue
                    // - pause and retry (backoff) - maybe set a flag to slow down the publisher
                    @Override
                    public void handleErrorEx(Object key, JCSMPException cause, long timestamp) {
                        if (key != null) {
                            @SuppressWarnings("unchecked")
                            Pair<XMLMessage,Topic> messageAndTopic = (Pair<XMLMessage,Topic>)key;                
                            try {
                                // For this example we will resend the message from the callback, because the main thread may
                                // have already called producer.send, there is a chance this resent message will be out-of-sequence
                                producer.send(messageAndTopic.getFirst(), messageAndTopic.getSecond());
                                logger.info("Message [{}] Resent", messageAndTopic.getFirst().getApplicationMessageId());
                            } catch (JCSMPException e) {
                                // Something went really wrong.... for this example we will just give up
                                session.closeSession();
                            }
                        } else {  // not a NACK, but some other error (ACL violation, connection loss, message too big, ...)
                            logger.warn("### Producer handleErrorEx() callback: %s%n", cause);
                            if (cause instanceof JCSMPTransportException) {  // all reconnect attempts failed
                                session.closeSession();
                            } else if (cause instanceof JCSMPErrorResponseException) {  // might have some extra info
                                JCSMPErrorResponseException e = (JCSMPErrorResponseException)cause;
                                logger.warn("Specifics: " + JCSMPErrorResponseSubcodeEx.getSubcodeAsString(e.getSubcodeEx()) + ": " + e.getResponsePhrase());
                            }
                        }
                    }
                },
                new JCSMPProducerEventHandler() {
                    @Override
                    public void handleEvent(ProducerEventArgs event) {
                        // as of JCSMP v10.10, this event only occurs when republishing unACKed messages
                        // on an unknown flow (DR failover)
                        logger.info("*** Received a producer event: " + event);
                    }
                });
        }
        private XMLMessage createMessage(byte[] payload) throws SDTException {
            BytesMessage message = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
            message.setData(payload);
            message.setDeliveryMode(DeliveryMode.PERSISTENT);  // required for Guaranteed
            message.setApplicationMessageId(String.format("%08d", sequenceNumber));  // as an example
            message.setSequenceNumber(sequenceNumber);

            // as another example, let's define a user property!
            SDTMap map = JCSMPFactory.onlyInstance().createMap();
            map.putString("sample", API + "_" + SAMPLE_NAME);
            message.setProperties(map);
            return message;
        }
    }
    private static class BlockingMessagePublisher implements Publisher {
        private static final int PUBLISH_WINDOW_SIZE = 1;
        private static final int CHANNEL_RECONNECT_RETRIES = 20;
        private static final int CHANNEL_CONNECT_RETRIES_PER_HOST = 5;

        private JCSMPProperties sessionProps;
        private JCSMPSession session;
        private XMLMessageProducer producer;
        private int sequenceNumber = 0;

        public BlockingMessagePublisher(String host, String vpnName, String userName, String password) {
            this.sessionProps = new JCSMPProperties();
            sessionProps.setProperty(JCSMPProperties.HOST, host);             // host:port
            sessionProps.setProperty(JCSMPProperties.VPN_NAME,  vpnName);     // message-vpn
            sessionProps.setProperty(JCSMPProperties.USERNAME, userName);     // client-username
            if (password.compareTo("") != 0) {
                sessionProps.setProperty(JCSMPProperties.PASSWORD, password); // client-password
            }
            sessionProps.setProperty(JCSMPProperties.PUB_ACK_WINDOW_SIZE, PUBLISH_WINDOW_SIZE);

            JCSMPChannelProperties channelProps = new JCSMPChannelProperties();
            channelProps.setReconnectRetries(CHANNEL_RECONNECT_RETRIES);      // recommended settings
            channelProps.setConnectRetriesPerHost(CHANNEL_CONNECT_RETRIES_PER_HOST);  // recommended settings
            sessionProps.setProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES, channelProps);
        }

        public void connect() throws JCSMPException {
            this.session = JCSMPFactory.onlyInstance().createSession(this.sessionProps, null, new SessionEventHandler() {
                @Override
                public void handleEvent(SessionEventArgs event) {  // could be reconnecting, connection lost, etc.
                    logger.info("### Received a Session event: " + event);
                }
            });
            this.session.connect();
            this.producer = this.createMessageProducer();
        }

        public void sendMessage(byte[] payload, String topicString) throws JCSMPException {
            XMLMessage message = this.createMessage(payload);
            Topic topic = JCSMPFactory.onlyInstance().createTopic(topicString);
            
            while(true) {
                try {
                    logger.info("Message [{}] Sending...", message.getApplicationMessageId());
                    
                    CompletableFuture<Boolean> publishResult = new CompletableFuture<Boolean>();
                    message.setCorrelationKey(publishResult);  // used for ACK/NACK correlation locally within the API
                    this.producer.send(message, topic);
                    
                    logger.info("Message [{}] Sent", message.getApplicationMessageId());
                    
                    // Here we block the calling thread until the current publish operation succeeds
                    // this is a more direct approach to one-by-one messaging and prevents out-of-order messaging
                    // at the tradeoff of lower message throughput
                    Boolean published = publishResult.get();
                    if(published) {
                        logger.info("Message [{}] Accepted", message.getApplicationMessageId());
                        break;
                    } else {
                        logger.info("Message [{}] Rejected", message.getApplicationMessageId());
                        
                        // Optionally add variable time delay to acheive exponential backoff...
                    }
                } catch (Exception ex) {
                    this.session.closeSession();
                }
            }            
            sequenceNumber++;
        }

        public void close() {
            this.session.closeSession();
        }

        private XMLMessageProducer createMessageProducer() throws JCSMPException {
            return this.session.getMessageProducer(
                new JCSMPStreamingPublishCorrelatingEventHandler() {
                    @Override
                    public void responseReceivedEx(Object key) {
                        if (key != null) {
                            @SuppressWarnings("unchecked")
                            CompletableFuture<Boolean> future = (CompletableFuture<Boolean>)key;
                            future.complete(true);
                        }
                    }

                    // Here's where we handle failed publish attempts, there are many options on what to do next:
                    // - send the message again
                    // - send it somewhere else (e.g., error handling queue)
                    // - log and continue
                    // - pause and retry (backoff) - maybe set a flag to slow down the publisher
                    @Override
                    public void handleErrorEx(Object key, JCSMPException cause, long timestamp) {
                        if (key != null) {
                            @SuppressWarnings("unchecked")
                            CompletableFuture<Boolean> future = (CompletableFuture<Boolean>)key;
                            future.complete(false);
                        } else {  // not a NACK, but some other error (ACL violation, connection loss, message too big, ...)
                            logger.warn("### Producer handleErrorEx() callback: %s%n", cause);
                            if (cause instanceof JCSMPTransportException) {  // all reconnect attempts failed
                                session.closeSession();
                            } else if (cause instanceof JCSMPErrorResponseException) {  // might have some extra info
                                JCSMPErrorResponseException e = (JCSMPErrorResponseException)cause;
                                logger.warn("Specifics: " + JCSMPErrorResponseSubcodeEx.getSubcodeAsString(e.getSubcodeEx()) + ": " + e.getResponsePhrase());
                            }
                        }
                    }
                },
                new JCSMPProducerEventHandler() {
                    @Override
                    public void handleEvent(ProducerEventArgs event) {
                        // as of JCSMP v10.10, this event only occurs when republishing unACKed messages
                        // on an unknown flow (DR failover)
                        logger.info("*** Received a producer event: " + event);
                    }
                });
        }
        private XMLMessage createMessage(byte[] payload) throws SDTException {
            BytesMessage message = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
            message.setData(payload);
            message.setDeliveryMode(DeliveryMode.PERSISTENT);  // required for Guaranteed
            message.setApplicationMessageId(String.format("%08d", sequenceNumber));  // as an example
            message.setSequenceNumber(sequenceNumber);

            // as another example, let's define a user property!
            SDTMap map = JCSMPFactory.onlyInstance().createMap();
            map.putString("sample", API + "_" + SAMPLE_NAME);
            message.setProperties(map);
            return message;
        }
    }
    private static class TransactedPublisher implements Publisher {
        private static final int PUBLISH_WINDOW_SIZE = 20;
        private static final int CHANNEL_RECONNECT_RETRIES = 20;
        private static final int CHANNEL_CONNECT_RETRIES_PER_HOST = 5;
        private static final int TRANSACTION_SIZE = 10;

        private JCSMPProperties sessionProps;
        private JCSMPSession session;
        private XMLMessageProducer producer;
        private int sequenceNumber = 0;

        private TransactedSession transactedSession;
        private ArrayList<Pair<XMLMessage,Topic>> pendingMessages;

        public TransactedPublisher(String host, String vpnName, String userName, String password) {
            this.sessionProps = new JCSMPProperties();
            sessionProps.setProperty(JCSMPProperties.HOST, host);             // host:port
            sessionProps.setProperty(JCSMPProperties.VPN_NAME,  vpnName);     // message-vpn
            sessionProps.setProperty(JCSMPProperties.USERNAME, userName);     // client-username
            if (password.compareTo("") != 0) {
                sessionProps.setProperty(JCSMPProperties.PASSWORD, password); // client-password
            }
            sessionProps.setProperty(JCSMPProperties.PUB_ACK_WINDOW_SIZE, PUBLISH_WINDOW_SIZE);

            JCSMPChannelProperties channelProps = new JCSMPChannelProperties();
            channelProps.setReconnectRetries(CHANNEL_RECONNECT_RETRIES);      // recommended settings
            channelProps.setConnectRetriesPerHost(CHANNEL_CONNECT_RETRIES_PER_HOST);  // recommended settings
            sessionProps.setProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES, channelProps);

            this.pendingMessages = new ArrayList<>(TRANSACTION_SIZE);
        }

        public void connect() throws JCSMPException {
            this.session = JCSMPFactory.onlyInstance().createSession(this.sessionProps, null, new SessionEventHandler() {
                @Override
                public void handleEvent(SessionEventArgs event) {  // could be reconnecting, connection lost, etc.
                    logger.info("### Received a Session event: " + event);
                }
            });
            this.session.connect();
            this.producer = this.createMessageProducer();
        }

        public void sendMessage(byte[] payload, String topicString) throws JCSMPException {
            XMLMessage message = createMessage(payload);
            Topic topic = JCSMPFactory.onlyInstance().createTopic(topicString);
            
            logger.info("Message [{}] Sending...", message.getApplicationMessageId());

            message.setCorrelationKey(new Pair<XMLMessage,Topic>(message, topic));  // used for ACK/NACK correlation locally within the API
            this.pendingMessages.add(new Pair<XMLMessage,Topic>(message, topic));
            this.producer.send(message, topic);
            
            logger.info("Message [{}] Sent", message.getApplicationMessageId());
            
            if(this.pendingMessages.size() == TRANSACTION_SIZE) {
                this.commitMessagesWithRetry();
                logger.info("Message [{}] Committed\n", message.getApplicationMessageId());
            }
            sequenceNumber++;
        }

        public void close() {
            this.session.closeSession();
        }

        private void commitMessagesWithRetry() throws JCSMPException {
            while(true) {
                try {
                    this.transactedSession.commit();
                    this.pendingMessages.clear();
                    return;
                } catch (RollbackException | TransactionResultUnknownException ex) {
                    // Could also implement exponential delay with max retries before aborting...
                    for(Pair<XMLMessage,Topic> messageAndTopic : this.pendingMessages) {
                        this.producer.send(messageAndTopic.getFirst(), messageAndTopic.getSecond());
                    }
                }
            }
        }

        private XMLMessage createMessage(byte[] payload) throws SDTException {
            BytesMessage message = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
            message.setData(payload);
            message.setDeliveryMode(DeliveryMode.PERSISTENT);  // required for Guaranteed
            message.setApplicationMessageId(String.format("%08d", sequenceNumber));  // as an example
            message.setSequenceNumber(sequenceNumber);

            // as another example, let's define a user property!
            SDTMap map = JCSMPFactory.onlyInstance().createMap();
            map.putString("sample", API + "_" + SAMPLE_NAME);
            message.setProperties(map);
            return message;
        }

        private XMLMessageProducer createMessageProducer() throws JCSMPException {
            this.transactedSession = this.session.createTransactedSession();
            ProducerFlowProperties flowProperties = new ProducerFlowProperties();

            var eventHandler = new JCSMPStreamingPublishCorrelatingEventHandler() {
                @Override
                public void responseReceivedEx(Object key) {
                    throw new RuntimeException("Received unexpected message ACK from a transacted session.");
                }

                @Override
                public void handleErrorEx(Object key, JCSMPException cause, long timestamp) {
                    if (key != null) {
                        throw new RuntimeException("Received unexpected message NAK from a transacted session.");
                    } else { // not a NACK, but some other error (ACL violation, connection loss, message too
                             // big, ...)
                        logger.warn("### Producer handleErrorEx() callback: %s%n", cause);
                        if (cause instanceof JCSMPTransportException) { // all reconnect attempts failed
                            session.closeSession();
                        } else if (cause instanceof JCSMPErrorResponseException) { // might have some extra info
                            JCSMPErrorResponseException e = (JCSMPErrorResponseException) cause;
                            logger.warn("Specifics: " + JCSMPErrorResponseSubcodeEx.getSubcodeAsString(e.getSubcodeEx())
                                    + ": " + e.getResponsePhrase());
                        }
                    }
                }
            };

            this.session.getMessageProducer(eventHandler); // <- not sure why this is needed, but it is...
            return this.transactedSession.createProducer(flowProperties, eventHandler);
        }
    }    
}
