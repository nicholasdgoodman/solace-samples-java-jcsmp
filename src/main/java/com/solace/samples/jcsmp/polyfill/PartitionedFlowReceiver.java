package com.solace.samples.jcsmp.polyfill;

import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.Endpoint;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.FlowEvent;
import com.solacesystems.jcsmp.FlowEventArgs;
import com.solacesystems.jcsmp.FlowEventHandler;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPInterruptedException;
import com.solacesystems.jcsmp.JCSMPLogLevel;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.Subscription;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageListener;
import com.solacesystems.jcsmp.XMLMessageProducer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.HashMap;


public class PartitionedFlowReceiver {
  private static final Logger logger = LogManager.getLogger();
  
  // Sample creation of wrapped FlowReceiver which leverages a PartitionAssignmentManager internally
  // Currently only implements FlowReceiver.start();
  // TODO Implement other functions where applicable - as needed
  public static FlowReceiver createFlow(
    JCSMPSession session,
    XMLMessageListener listener,
    ConsumerFlowProperties flowProperties,
    EndpointProperties endpointProperties,
    FlowEventHandler flowEventHandler,
    int partitionCount) throws JCSMPException {
      PartitionAssignmentManager manager = new JcsmpPartitionAssignmentManager(session, listener, flowProperties, endpointProperties, flowEventHandler, partitionCount);
    return new FlowReceiver() {
      @Override
      public void close() {
        // TODO Auto-generated method stub
      }

      @Override
      public void closeSync() throws JCSMPException {
        // TODO Auto-generated method stub
        
      }

      @Override
      public void closeSync(boolean arg0) throws JCSMPException {
        // TODO Auto-generated method stub
        
      }

      @Override
      public boolean isClosed() {
        // TODO Auto-generated method stub
        return false;
      }

      @Override
      public BytesXMLMessage receive() throws JCSMPException {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public BytesXMLMessage receive(int timeoutInMillis) throws JCSMPException {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public BytesXMLMessage receiveNoWait() throws JCSMPException {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public void start() throws JCSMPException {
        manager.start();        
      }

      @Override
      public void startSync() throws JCSMPException {
        // TODO Auto-generated method stub
        
      }

      @Override
      public void stop() {
        // TODO Auto-generated method stub
        
      }

      @Override
      public void stopSync() throws JCSMPInterruptedException {
        // TODO Auto-generated method stub
        
      }

      @Override
      public boolean stopSyncStart() {
        // TODO Auto-generated method stub
        return false;
      }

      @Override
      public void stopSyncWait() throws JCSMPInterruptedException {
        // TODO Auto-generated method stub
        
      }

      @Override
      public void close(boolean arg0) {
        // TODO Auto-generated method stub
        
      }

      @Override
      public Destination getDestination() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public Endpoint getEndpoint() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public Subscription getSubscription() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public void logFlowInfo(JCSMPLogLevel level) {
        // TODO Auto-generated method stub
        
      }

      @Override
      public void setMessageListener(XMLMessageListener arg0) {
        // TODO Auto-generated method stub
        
      }
      
    };
  }
  private static class JcsmpPartitionAssignmentManager extends PartitionAssignmentManager {
    private JCSMPSession session;
    private XMLMessageListener messageListener;
    private String ackMode;
    private EndpointProperties endpointProperties;
    private DataFlowEventHandler dataFlowEventHandler;

    private XMLMessageConsumer cmdConsumer;
    private XMLMessageProducer cmdProducer;
    private FlowReceiver mgmtFlowReceiver;
    private HashMap<String,FlowReceiver> dataFlowReceivers;

    public JcsmpPartitionAssignmentManager(
      JCSMPSession session,
      XMLMessageListener listener,
      ConsumerFlowProperties flowProperties,
      EndpointProperties endpointProperties,
      FlowEventHandler flowEventHandler,
      int partitionCount) {
        super(flowProperties.getEndpoint().getName(), partitionCount);
        this.session = session;
        this.messageListener = listener;
        this.ackMode = flowProperties.getAckMode();
        this.endpointProperties = endpointProperties;
        this.dataFlowEventHandler = new DataFlowEventHandler(flowEventHandler);
    }
    @Override
    public void start() {
      try {
        this.cmdConsumer = this.session.getMessageConsumer(new CommandListener(this));
        this.cmdProducer = this.session.getMessageProducer(new CommandProducerEventHandler());

        Queue mgmtQueue = JCSMPFactory.onlyInstance().createQueue(this.queueName);
        ConsumerFlowProperties mgmtFlowProperties = new ConsumerFlowProperties();
        mgmtFlowProperties.setEndpoint(mgmtQueue);
        mgmtFlowProperties.setActiveFlowIndication(true);

        this.dataFlowReceivers = new HashMap<>();
        this.mgmtFlowReceiver = session.createFlow(new MgmtListener(), mgmtFlowProperties, null, new MgmtFlowEventHandler(this));
        this.mgmtFlowReceiver.start();
      } catch (JCSMPException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }

      super.start();
    }
    @Override
    void addSubscriptions(String stateTopic, String rebalanceTopic) {
      logger.info("addSubscriptions({},{})", stateTopic, rebalanceTopic);
      try {
        this.session.addSubscription(JCSMPFactory.onlyInstance().createTopic(stateTopic));
        this.session.addSubscription(JCSMPFactory.onlyInstance().createTopic(rebalanceTopic));
        this.cmdConsumer.start();
      } catch (JCSMPException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    @Override
    void sendCommand(String topic, byte[] data) {
      BytesMessage message = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
      message.setData(data);
      try {
        cmdProducer.send(message, JCSMPFactory.onlyInstance().createTopic(topic));
      } catch (JCSMPException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    @Override
    void connectToPartition(String partitionQueueName) {
      try {
        FlowReceiver dataFlow = this.createDataFlow(partitionQueueName);
        dataFlow.start();
        this.dataFlowReceivers.put(partitionQueueName, dataFlow);
      } catch (JCSMPException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    @Override
    void disconnectFromPartition(String partitionQueueName) {
      try {
        FlowReceiver dataFlow = this.dataFlowReceivers.get(partitionQueueName);
        // TODO put delay between stopping and closing to mitigate redelivery
        dataFlow.stop();
        dataFlow.close();
        this.dataFlowReceivers.remove(partitionQueueName);
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }     
    }

    private FlowReceiver createDataFlow(String dataQueueName) throws JCSMPException {
      Queue dataQueue = JCSMPFactory.onlyInstance().createQueue(dataQueueName);
      ConsumerFlowProperties dataFlowProperties = new ConsumerFlowProperties();
      dataFlowProperties.setEndpoint(dataQueue);
      dataFlowProperties.setAckMode(this.ackMode);
      dataFlowProperties.setActiveFlowIndication(true);
  
      logger.info("createDataFlow: {}", dataQueueName);
  
      return this.session.createFlow(
        this.messageListener,
        dataFlowProperties,
        this.endpointProperties,
        this.dataFlowEventHandler
      );
    }
    private static class MgmtListener implements XMLMessageListener {
      @Override
      public void onException(JCSMPException exception) { }
      @Override
      public void onReceive(BytesXMLMessage message) { }

    }
    private static class CommandListener implements XMLMessageListener {
      private PartitionAssignmentManager manager;
      CommandListener(PartitionAssignmentManager manager) {
        this.manager = manager;
      }
      @Override
      public void onReceive(BytesXMLMessage message) {
        String destination = message.getDestination().getName();
        ByteBuffer data = message.getAttachmentByteBuffer();
        manager.processCommand(destination, data);
      }
      @Override
      public void onException(JCSMPException exception) {
        logger.error(exception);
      }
    }
    private static class CommandProducerEventHandler implements JCSMPStreamingPublishCorrelatingEventHandler {
      @Override
      public void handleErrorEx(Object key, JCSMPException cause, long timestamp) {}
      @Override
      public void responseReceivedEx(Object key) {}
    }
    private static class MgmtFlowEventHandler implements FlowEventHandler {
      private PartitionAssignmentManager manager;
  
      public MgmtFlowEventHandler(PartitionAssignmentManager manager){
        this.manager = manager;
      }
      @Override
      public void handleEvent(Object source, FlowEventArgs event) {
        if(event.getEvent() == FlowEvent.FLOW_ACTIVE) {
          logger.info("This Node is now the leader!");
          manager.setLeader(true);
        }
        if(event.getEvent() == FlowEvent.FLOW_INACTIVE) {
          logger.info("This Node has somehow lost its leader status!");
          manager.setLeader(false);
        }
      }
    }
    private static class DataFlowEventHandler implements FlowEventHandler {
      private FlowEventHandler flowEventHandler;
      private int activeFlowCount;
      public DataFlowEventHandler(FlowEventHandler flowEventHandler) {
        this.flowEventHandler = flowEventHandler;
        this.activeFlowCount = 0;
      }
      @Override
      public void handleEvent(Object source, FlowEventArgs event) {
        if(event.getEvent() == FlowEvent.FLOW_ACTIVE && this.activeFlowCount == 0) {
          this.activeFlowCount++;
          flowEventHandler.handleEvent(source, event);
        }
        if(event.getEvent() == FlowEvent.FLOW_INACTIVE) {
          this.activeFlowCount--;
          if(this.activeFlowCount == 0) {
            flowEventHandler.handleEvent(source, event);
          }
        }
      }
    }
  
  }
}
