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
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.FlowEvent;
import com.solacesystems.jcsmp.FlowEventArgs;
import com.solacesystems.jcsmp.FlowEventHandler;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPChannelProperties;
import com.solacesystems.jcsmp.JCSMPErrorResponseException;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPTransportException;
import com.solacesystems.jcsmp.OperationNotSupportedException;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.SessionEventArgs;
import com.solacesystems.jcsmp.SessionEventHandler;
import com.solacesystems.jcsmp.XMLMessageListener;
import com.solacesystems.jcsmp.impl.flow.FlowHandleImpl;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.solace.samples.jcsmp.polyfill.PartitionedFlowReceiver;
import com.solace.samples.jcsmp.utils.OrderedExecutorService;

public class GuaranteedSubscriber {
  
  private static final String SAMPLE_NAME = GuaranteedSubscriber.class.getSimpleName();
  private static final String QUEUE_NAME = "q_jcsmp_sub";
    private static final String API = "JCSMP";
    private static final int PARTITION_COUNT = 10;
    private static final int THREAD_COUNT = 4;
    
    private static volatile int msgRecvCounter = 0;                 // num messages received
    private static volatile int redeliveredMsgCounter = 0;  // detected any messages being redelivered?
    private static volatile int failedMsgAckCounter = 0;
    private static volatile boolean isShutdown = false;             // are we done?
    private static FlowReceiver flowQueueReceiver;

    // remember to add log4j2.xml to your classpath
    private static final Logger logger = LogManager.getLogger(GuaranteedSubscriber.class);  // log4j2, but could also use SLF4J, JCL, etc.

    /** This is the main app.  Use this type of app for receiving Guaranteed messages (e.g. via a queue endpoint). */
    public static void main(String... args) throws JCSMPException, InterruptedException, IOException {
        if (args.length < 3) {  // Check command line arguments
            System.out.printf("Usage: %s <host:port> <message-vpn> <client-username> [password]%n%n", SAMPLE_NAME);
            System.exit(-1);
        }
        System.out.println(API + " " + SAMPLE_NAME + " initializing...");
        String clientName = ManagementFactory.getRuntimeMXBean().getName();

        final JCSMPProperties properties = new JCSMPProperties();
        properties.setProperty(JCSMPProperties.HOST, args[0]);          // host:port
        properties.setProperty(JCSMPProperties.VPN_NAME,  args[1]);     // message-vpn
        properties.setProperty(JCSMPProperties.USERNAME, args[2]);      // client-username
        if (args.length > 3) {
            properties.setProperty(JCSMPProperties.PASSWORD, args[3]);  // client-password
        }
        properties.setProperty(JCSMPProperties.CLIENT_NAME, clientName);
        properties.setProperty(JCSMPProperties.SUB_ACK_WINDOW_SIZE, 100);

        JCSMPChannelProperties channelProps = new JCSMPChannelProperties();
        channelProps.setReconnectRetries(20);      // recommended settings
        channelProps.setConnectRetriesPerHost(5);  // recommended settings
        // https://docs.solace.com/Solace-PubSub-Messaging-APIs/API-Developer-Guide/Configuring-Connection-T.htm
        properties.setProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES, channelProps);
        final JCSMPSession session;
        session = JCSMPFactory.onlyInstance().createSession(properties, null, new SessionEventHandler() {
            @Override
            public void handleEvent(SessionEventArgs event) {  // could be reconnecting, connection lost, etc.
                logger.info("### Received a Session event: [{}]", event);
            }
        });
        session.connect();

        // configure the queue API object locally
        final Queue queue = JCSMPFactory.onlyInstance().createQueue(QUEUE_NAME);
        // Create a Flow be able to bind to and consume messages from the Queue.
        final ConsumerFlowProperties flow_prop = new ConsumerFlowProperties();
        flow_prop.setEndpoint(queue);
        flow_prop.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);  // best practice
        flow_prop.setActiveFlowIndication(true);  // Flow events will advise when 
        
        System.out.printf("Attempting to bind to queue '%s' on the broker.%n", QUEUE_NAME);
        try {
            // This listener serves as both he Message Handler and Flow Event Handler
            MultiThreadedMessageListener messageAndEventListener = new MultiThreadedMessageListener(PARTITION_COUNT);
            flowQueueReceiver = PartitionedFlowReceiver.createFlow(session,
                messageAndEventListener, flow_prop, null, messageAndEventListener, PARTITION_COUNT);
        } catch (OperationNotSupportedException e) {  // not allowed to do this
            throw e;
        } catch (JCSMPErrorResponseException e) {  // something else went wrong: queue not exist, queue shutdown, etc.
            logger.error(e);
            System.err.printf("%n*** Could not establish a connection to queue '%s': %s%n", QUEUE_NAME, e.getMessage());
            System.err.println("Create queue using PubSub+ Manager WebGUI, and add subscription "+
                    GuaranteedPublisher.TOPIC_PREFIX+"*/pers/>");
            System.err.println("  or see the SEMP CURL scripts inside the 'semp-rest-api' directory.");
            // could also try to retry, loop and retry until successfully able to connect to the queue
            System.err.println("NOTE: see QueueProvision sample for how to construct queue with consumer app.");
            System.err.println("Exiting.");
            return;
        }
        // tell the broker to start sending messages on this queue receiver
        flowQueueReceiver.start();
        // async queue receive working now, so time to wait until done...
        System.out.println(SAMPLE_NAME + " connected, and running. Press [ENTER] to quit.");
        while (System.in.available() == 0 && !isShutdown) {
            Thread.sleep(1000);  // wait 1 second
            System.out.printf("%s %s Received msgs/s: %d%n", API, SAMPLE_NAME, msgRecvCounter);  // simple way of calculating message rates
            msgRecvCounter = 0;
            if (redeliveredMsgCounter > 0) {  // try shutting -> enabling the queue on the broker to see this
                System.out.printf("*** Redeliveries detected: %d ***%n", redeliveredMsgCounter);
                redeliveredMsgCounter = 0;  // only show the error once per second
            }
            if (failedMsgAckCounter > 0) {
                System.out.printf("*** Failed message acks detected: %d ***%n", failedMsgAckCounter);
                failedMsgAckCounter = 0;
            }
        }
        isShutdown = true;
        flowQueueReceiver.stop();
        Thread.sleep(1000);
        session.closeSession();  // will also close consumer object
        System.out.println("Main thread quitting.");
    }

    ////////////////////////////////////////////////////////////////////////////

    /** Very simple static inner class, used for receives messages from Queue Flows. **/
    private static class MultiThreadedMessageListener implements XMLMessageListener, FlowEventHandler {
        private static final String QUEUE_PARTITION_ID = "_compat__kafka_receivedPartitionId";

        private final Pattern flowEventPartitionPattern = Pattern.compile("^\\[Partition (\\d+)\\]");
        private final OrderedExecutorService executorService;

        public MultiThreadedMessageListener(int nThreads) {
            this.executorService = OrderedExecutorService.newFixedThreadPool(nThreads);
        }

        @Override
        public void onReceive(BytesXMLMessage msg) {
            msgRecvCounter++;
            if (msg.getRedelivered()) {  // useful check
                // this is the broker telling the consumer that this message has been sent and not ACKed before.
                // this can happen if an exception is thrown, or the broker restarts, or the netowrk disconnects
                // perhaps an error in processing? Should do extra checks to avoid duplicate processing
                redeliveredMsgCounter++;
            }

            // Dispatch messages to individual executors for parallel processing
            Integer partitionIdProperty = null;
            try {
                partitionIdProperty = msg.getProperties().getInteger(QUEUE_PARTITION_ID);
            } catch (Exception ex) {
                // Do nothing! just assume there is no key set
            }
            final int partitionId = partitionIdProperty == null ? Integer.MIN_VALUE : partitionIdProperty.intValue();

            // Define the message processor:
            Runnable processMessage = new Runnable() {
                @Override
                public void run() { 
                    try {
                        // Simulate a long-running process
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    
                    // Messages are removed from the broker queue when the ACK is received.
                    // Therefore, DO NOT ACK until all processing/storing of this message is complete.
                    // NOTE that messages can be acknowledged from a different thread.
                    try {
                        msg.ackMessage();  // ACKs are asynchronous
                    } catch (Exception e) {
                        failedMsgAckCounter++;
                    }
                }
            };

            executorService.execute(processMessage, partitionId);
        }

        @Override
        public void onException(JCSMPException e) {
            logger.warn("### Queue " + QUEUE_NAME + " Flow handler received exception.  Stopping!!", e);
            if (e instanceof JCSMPTransportException) {  // all reconnect attempts failed
                isShutdown = true;  // let's quit; or, could initiate a new connection attempt
            } else {
                // Generally unrecoverable exception, probably need to recreate and restart the flow
                flowQueueReceiver.close();
                // add logic in main thread to restart FlowReceiver, or can exit the program
            }
        }

        @Override
        public void handleEvent(Object source, FlowEventArgs eventArgs) {
            // Flow events are usually: active, reconnecting (i.e. unbound), reconnected, active
            // try disabling and re-enabling the queue to see in action
            logger.info("### Received a Flow event: [{}]", eventArgs);
            FlowHandleImpl flowHandle = (FlowHandleImpl)source;
            FlowEvent event = eventArgs.getEvent();
            String info = eventArgs.getInfo();

            Matcher matcher = flowEventPartitionPattern.matcher(info);
            
            Integer partitionId = matcher.find() ? Integer.valueOf(matcher.group(1)) : null;

            if(partitionId == null) {
                logger.warn("Unable to determine source partition for flow event.");
                return;
            }

            if(flowHandle.isClosed() || event == FlowEvent.FLOW_DOWN) {
                logger.info("Partition is disconnected. Purging queued messages for partition {}", partitionId);
                List<Runnable> abortedTasks = executorService.shutdownNow(partitionId);
                logger.info("{} prefetched messages were discarded and not processed.", abortedTasks.size());
                executorService.reset(partitionId);
            } else if(flowHandle.isStoppedByApplication() || event == FlowEvent.FLOW_RECONNECTING) {
                logger.info("Partition flow is stopped. Stopping processing on partition {}", partitionId);
                executorService.stop(partitionId);
            } else if(event == FlowEvent.FLOW_RECONNECTED) {
                logger.info("Partition is reconnected. Starting processing on partition {}", partitionId);
                executorService.start(partitionId);
            }
        }
    }
}
