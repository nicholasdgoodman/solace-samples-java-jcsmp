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
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.Subscription;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageListener;
import com.solacesystems.jcsmp.XMLMessageProducer;
import com.solacesystems.jcsmp.impl.flow.FlowHandleImpl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class PartitionedFlowReceiver {
  private static final Logger LOGGER = LogManager.getLogger(PartitionedFlowReceiver.class);
  private static final int PARTITION_SHUTDOWN_TIME_MS = 2000;
  
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
    private FlowEventHandler flowEventHandler;

    private XMLMessageConsumer cmdConsumer;
    private XMLMessageProducer cmdProducer;
    private FlowReceiver mgmtFlowReceiver;
    private HashMap<Integer,FlowHandleImpl> dataFlowReceivers;
    private ScheduledExecutorService shutdownScheduler;

    public JcsmpPartitionAssignmentManager(
      JCSMPSession session,
      XMLMessageListener listener,
      ConsumerFlowProperties flowProperties,
      EndpointProperties endpointProperties,
      FlowEventHandler flowEventHandler,
      int partitionCount) {
        super(
            (String)session.getProperty(JCSMPProperties.VIRTUAL_ROUTER_NAME),
            (String)session.getProperty(JCSMPProperties.VPN_NAME),
            flowProperties.getEndpoint().getName(),
            (String)session.getProperty(JCSMPProperties.CLIENT_NAME),
            partitionCount);
        this.session = session;
        this.messageListener = listener;
        this.ackMode = flowProperties.getAckMode();
        this.endpointProperties = endpointProperties;
        this.flowEventHandler = flowEventHandler;
        this.shutdownScheduler = Executors.newScheduledThreadPool(partitionCount);
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
      LOGGER.info("addSubscriptions({},{})", stateTopic, rebalanceTopic);
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
      System.out.print("+");
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
    void connectToPartition(Integer partitionId) {
      LOGGER.info("Connecting to queue {}, partition {}", queueName, partitionId);
      try {
        FlowHandleImpl dataFlow = this.createDataFlow(partitionId);
        dataFlow.start();
        this.dataFlowReceivers.put(partitionId, dataFlow);
      } catch (JCSMPException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    @Override
    void disconnectFromPartition(Integer partitionId) {
      LOGGER.info("Disconnecting from queue {}, partition {}", queueName, partitionId);
      try {
        FlowHandleImpl dataFlow = this.dataFlowReceivers.get(partitionId);
        FlowEventHandler dataFlowEventHandler = dataFlow.getFlowEventHandler();

        LOGGER.debug("Stopping flow from queue {}, partition {}", queueName, partitionId);
        dataFlow.stop();
        
        // Raise a synthetic event for downstream consumers to respond to
        dataFlowEventHandler.handleEvent(dataFlow, new FlowEventArgs(FlowEvent.FLOW_INACTIVE, "Partition is disconnecting", null, 0));
        shutdownScheduler.schedule(() -> closePartition(partitionId), PARTITION_SHUTDOWN_TIME_MS, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }     
    }

    private void closePartition(Integer partitionId) {
        FlowHandleImpl dataFlow = this.dataFlowReceivers.get(partitionId);
        FlowEventHandler dataFlowEventHandler = dataFlow.getFlowEventHandler();

        // Raise a synthetic event for downstream consumers to respond to
        LOGGER.debug("Closing flow from queue {}, partition {}", queueName, partitionId);
        dataFlow.close();
        dataFlowEventHandler.handleEvent(dataFlow, new FlowEventArgs(FlowEvent.FLOW_INACTIVE, "Partition was disconnected", null, 0));

        this.dataFlowReceivers.remove(partitionId);
    }

    private FlowHandleImpl createDataFlow(Integer partitionId) throws JCSMPException {
      String dataQueueName = this.getPartitionQueueName(partitionId);
      
      Queue dataQueue = JCSMPFactory.onlyInstance().createQueue(dataQueueName);
      ConsumerFlowProperties dataFlowProperties = new ConsumerFlowProperties();
      
      dataFlowProperties.setEndpoint(dataQueue);
      dataFlowProperties.setAckMode(this.ackMode);
      dataFlowProperties.setActiveFlowIndication(true);
  
      XMLMessageListener partitionedMessageListener = new PartitionedMessageListener(this.messageListener, partitionId);
      DataFlowEventHandler dataFlowEventHandler = new DataFlowEventHandler(this.flowEventHandler, partitionId);
  
      return  (FlowHandleImpl)this.session.createFlow(
        partitionedMessageListener,
        dataFlowProperties,
        this.endpointProperties,
        dataFlowEventHandler
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
        System.out.print(".");
        String destination = message.getDestination().getName();
        ByteBuffer data = message.getAttachmentByteBuffer();
        manager.processCommand(destination, data);
      }
      @Override
      public void onException(JCSMPException exception) {
        LOGGER.error(exception);
      }
    }
    private static class CommandProducerEventHandler implements JCSMPStreamingPublishCorrelatingEventHandler {
      @Override
      public void handleErrorEx(Object key, JCSMPException cause, long timestamp) {}
      @Override
      public void responseReceivedEx(Object key) {}
    }
    private static class PartitionedMessageListener implements XMLMessageListener {
      private static final String PARTITION_ID_HEADER = "_compat__kafka_receivedPartitionId";
      private XMLMessageListener listener;
      private Integer partitionId;

      public PartitionedMessageListener(XMLMessageListener listener, Integer partitionId) {
        this.listener = listener;
        this.partitionId = partitionId;
      }

      @Override
      public void onException(JCSMPException exception) {
        this.listener.onException(exception);
      }

      @Override
      public void onReceive(BytesXMLMessage message) {
        try {
          message.getProperties().putInteger(PARTITION_ID_HEADER, this.partitionId);
        } catch (Exception ex) {
          ex.printStackTrace();
        }
        this.listener.onReceive(message);
      }

    }
    private static class MgmtFlowEventHandler implements FlowEventHandler {
      private PartitionAssignmentManager manager;
  
      public MgmtFlowEventHandler(PartitionAssignmentManager manager){
        this.manager = manager;
      }
      @Override
      public void handleEvent(Object source, FlowEventArgs event) {
        if(event.getEvent() == FlowEvent.FLOW_ACTIVE) {
          LOGGER.info("This Node is now the leader!");
          manager.setLeader(true);
        }
        if(event.getEvent() == FlowEvent.FLOW_INACTIVE) {
          LOGGER.info("This Node has somehow lost its leader status!");
          manager.setLeader(false);
        }
      }
    }
    private static class DataFlowEventHandler implements FlowEventHandler {
      private FlowEventHandler flowEventHandler;
      private int partitionId;
      
      public DataFlowEventHandler(FlowEventHandler flowEventHandler, int partitionId) {
        this.flowEventHandler = flowEventHandler;
        this.partitionId = partitionId;
      }
      @Override
      public void handleEvent(Object source, FlowEventArgs eventArgs) {
        FlowHandleImpl flowHandle = (FlowHandleImpl)source;
        FlowEvent event = eventArgs.getEvent();
        String infoSt = eventArgs.getInfo();
        Exception ex = eventArgs.getException();
        int respCode = eventArgs.getResponseCode();

        infoSt = String.format("[Partition %d] %s", partitionId, infoSt);
        flowEventHandler.handleEvent(flowHandle, new FlowEventArgs(event, infoSt, ex, respCode));
      }
    }
  
  }
}
