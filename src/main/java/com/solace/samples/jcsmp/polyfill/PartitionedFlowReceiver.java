package com.solace.samples.jcsmp.polyfill;

import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.FlowEvent;
import com.solacesystems.jcsmp.FlowEventArgs;
import com.solacesystems.jcsmp.FlowEventHandler;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageListener;
import com.solacesystems.jcsmp.XMLMessageProducer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.text.MessageFormat;
import java.util.Arrays;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Stack;
import java.util.Timer;
import java.util.TimerTask;

import java.util.Map.Entry;
import java.util.stream.Collectors;

public class PartitionedFlowReceiver {
  private static final Logger logger = LogManager.getLogger();
  private static final String TOPIC_PREFIX_STATE = "#PQC/state/";
  private static final String TOPIC_PREFIX_REBALANCE = "#PQC/rebalance/";
  private static final int STATE_UPDATE_PERIOD = 1000;

  private FlowReceiver mgmtFlowReceiver;
  private HashMap<Integer,FlowReceiver> dataFlowReceivers;
  private FlowEventHandler flowEventHandler;
  private JCSMPSession session;
  private XMLMessageListener listener;
  private ConsumerFlowProperties flowProperties;
  private EndpointProperties endpointProperties;

  private XMLMessageConsumer cmdConsumer;
  private String instanceId;
  private int partitionCount;

  private boolean isLeader;
  private int stateHoldCount;
  private int[] connectedPartitions;

  private HashSet<Integer> assignedPartitions;
  private HashMap<String,int[]> globalConnectedPartitions;

  private Timer stateTimer;
  private XMLMessageProducer commandProducer;
  private String parentQueueName;

  private PartitionedFlowReceiver(JCSMPSession session, XMLMessageListener listener,
      ConsumerFlowProperties flowProperties, EndpointProperties endpointProperties, FlowEventHandler flowEventHandler) {
    this.session = session;
    this.listener = listener;
    this.flowProperties = flowProperties;
    this.endpointProperties = endpointProperties;
    this.flowEventHandler = flowEventHandler;
    this.instanceId = java.util.UUID.randomUUID().toString().substring(0,7);
    
    this.parentQueueName = flowProperties.getEndpoint().getName();
    this.assignedPartitions = new HashSet<>();
    this.connectedPartitions = new int[0];
    this.dataFlowReceivers = new HashMap<>();

    this.partitionCount = 6;
  }

  public static PartitionedFlowReceiver createFlow(
    JCSMPSession session,
    XMLMessageListener listener,
    ConsumerFlowProperties flowProperties,
    EndpointProperties endpointProperties,
    FlowEventHandler flowEventHandler) throws JCSMPException {
    
    PartitionedFlowReceiver receiver = new PartitionedFlowReceiver(session, listener, flowProperties, endpointProperties, flowEventHandler);
    receiver.createMgmtFlow();
    receiver.createCmdConsumer();
    receiver.createStatePublisher();

    return receiver;
  }

  public void start() throws JCSMPException {
    this.mgmtFlowReceiver.start();
    this.cmdConsumer.start();
    this.stateTimer.schedule(new StateProducerTimerTask(), STATE_UPDATE_PERIOD, STATE_UPDATE_PERIOD);
  }

  public void stop() {
    this.mgmtFlowReceiver.stop();
  }

  public void close() {
    this.mgmtFlowReceiver.close();
  }

  private void createMgmtFlow() throws JCSMPException {
    this.mgmtFlowReceiver = session.createFlow(
      new MessageListener(),
      flowProperties,
      endpointProperties,
      new MgmtFlowEventHandler()
    );
  }

  private FlowReceiver createDataFlow(int partitionId) throws JCSMPException {
    String queueName = flowProperties.getEndpoint().getName();
    String dataQueueName = String.join("/", queueName, Integer.toString(partitionId));
    Queue dataQueue = JCSMPFactory.onlyInstance().createQueue(dataQueueName);
    ConsumerFlowProperties dataFlowProperties = new ConsumerFlowProperties();
    dataFlowProperties.setEndpoint(dataQueue);
    dataFlowProperties.setAckMode(flowProperties.getAckMode());
    dataFlowProperties.setActiveFlowIndication(true);

    logger.info("createDataFlow: {}", dataQueueName);

    return session.createFlow(
      new MessageListener(),
      dataFlowProperties,
      endpointProperties,
      new DataFlowEventHandler(partitionId)
    );
  }

  private void createCmdConsumer() throws JCSMPException {
    this.cmdConsumer = this.session.getMessageConsumer(new CommandMessageListener());
    this.session.addSubscription(JCSMPFactory.onlyInstance().createTopic("#PQC/rebalance/" + this.instanceId));
    this.session.addSubscription(JCSMPFactory.onlyInstance().createTopic("#PQC/state/>"));
  }

  private void createStatePublisher() throws JCSMPException {
    this.commandProducer = session.getMessageProducer(new CommandMessageProducer());
    this.stateTimer = new Timer();
  }

  private void requestRebalance() {
    // Generate default partition-to-consumer mapping (all to leader)
    String[] partitionToConsumer = new String[this.partitionCount];
    Arrays.fill(partitionToConsumer, this.instanceId);
    HashMap<String,PartitionAssignments> assignmentMap = new HashMap<>();

    // Iterate over global state and update partition-to-consumer mapping
    for(Entry<String,int[]> entry : this.globalConnectedPartitions.entrySet()) {
      String consumerId = entry.getKey();
      assignmentMap.put(consumerId, new PartitionAssignments(consumerId));
      for(int partitionId : entry.getValue()) {
        partitionToConsumer[partitionId] = entry.getKey();
      }
    }
        
    // Map<string,Integer[]> for consumer-to-partition mapping
    for(int n = 0; n < partitionToConsumer.length; n++) {
      String consumerId = partitionToConsumer[n];
      PartitionAssignments assignments = assignmentMap.get(consumerId);
      assignments.addPartition(n);
      assignmentMap.put(consumerId, assignments);
    }

    // Perform re-assignments...
    PartitionAssignments[] orderedAssignments = assignmentMap.values().toArray(new PartitionAssignments[0]);
    int assignmentCount = orderedAssignments.length;

    int minAllowedCount = this.partitionCount / assignmentCount;
    int maxAllowedCount = minAllowedCount + ((this.partitionCount % assignmentCount > 0) ? 1 : 0);

    while(true) {
      Arrays.sort(orderedAssignments);
      PartitionAssignments minAssigned = orderedAssignments[0];
      PartitionAssignments maxAssigned = orderedAssignments[orderedAssignments.length - 1];
      int minAssignedCount = minAssigned.getPartitionCount();
      int maxAssignedCount = maxAssigned.getPartitionCount();

      if(minAssignedCount == minAllowedCount && maxAssignedCount == maxAllowedCount) {
        break;
      }

      minAssigned.addPartition(maxAssigned.removePartition());
    }

    for(PartitionAssignments assignments : orderedAssignments) {
      String id = assignments.getConsumerId();
      int[] partitions = assignments.getPartitions();
      BytesMessage message = partitionsToMessage(partitions);

      try {
        this.commandProducer.send(message, JCSMPFactory.onlyInstance().createTopic(TOPIC_PREFIX_REBALANCE + id));
      } catch (JCSMPException e) {
        // TODO What happens here? Give up?
      }
    }

    this.stateHoldCount = 0;
    this.globalConnectedPartitions.clear();
  }

  private void performRebalance(int[] partitionsToAssign) {
    Set<Integer> newAssignments = Arrays.stream(partitionsToAssign).boxed().collect(Collectors.toSet());    
    HashSet<Integer> toConnect = new HashSet<>(newAssignments);
    toConnect.removeAll(this.assignedPartitions);
    HashSet<Integer> toDisconnect = new HashSet<>(this.assignedPartitions);
    toDisconnect.removeAll(newAssignments);
    
    if(toConnect.size() > 0) {
      logger.info("Need to connect to: {}", toConnect);
    }
    if(toDisconnect.size() > 0) {
      logger.info("Need to disconnect from: {}", toDisconnect);
    }
    this.assignedPartitions.clear();
    this.assignedPartitions.addAll(newAssignments);

    // TODO Actual connection logic
    for(Integer partitionId : toConnect) {
      try {
        FlowReceiver dataFlow = createDataFlow(partitionId);
        dataFlow.start();
        this.dataFlowReceivers.put(partitionId, dataFlow);
      } catch (JCSMPException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    for(Integer partitionId : toDisconnect) {
      try {
        FlowReceiver dataFlow = this.dataFlowReceivers.get(partitionId);
        // TODO put delay between stopping and closing to mitigate redelivery
        dataFlow.stop();
        dataFlow.close();
        this.dataFlowReceivers.remove(partitionId);
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

    this.connectedPartitions = partitionsToAssign;
  }

  private static BytesMessage partitionsToMessage(int[] partitions) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(partitions.length * 4);        
    IntBuffer intBuffer = byteBuffer.asIntBuffer();
    intBuffer.put(partitions);
    byte[] messageData = byteBuffer.array();

    BytesMessage message = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
    message.setData(messageData);
    return message;
  }

  private static int[] messageToPartitions(BytesXMLMessage message) {
    ByteBuffer byteBuffer = message.getAttachmentByteBuffer();
    IntBuffer intBuffer = byteBuffer.asIntBuffer();
    int[] intData = new int[intBuffer.remaining()];
    intBuffer.get(intData);

    return intData;
  }

  private class PartitionAssignments implements Comparable<PartitionAssignments> {
    private String consumerId;
    private Stack<Integer> partitions;

    public PartitionAssignments(String consumerId) {
      this.consumerId = consumerId;
      this.partitions = new Stack<>();
    }

    public void addPartition(int partitionId) {
      partitions.push(partitionId);
    }

    public int removePartition() {
      return partitions.pop();
    }

    public String getConsumerId() {
      return consumerId;
    }
    public int[] getPartitions() {
      return partitions.stream().mapToInt(Integer::intValue).toArray();
    }
    public int getPartitionCount() {
      return partitions.size();
    }

    @Override
    public int compareTo(PartitionAssignments that) {
      int size1 = this.getPartitionCount();
      int size2 = that.getPartitionCount();
      int result = size1 - size2;

      return result;
    }
    @Override
    public String toString() {
      return MessageFormat.format("'{'{0}={1}'}'", this.consumerId, this.partitions);
    }
  }

  private class MgmtFlowEventHandler implements FlowEventHandler {

    @Override
    public void handleEvent(Object source, FlowEventArgs event) {
      flowEventHandler.handleEvent(source, event);

      if(event.getEvent() == FlowEvent.FLOW_ACTIVE) {
        logger.info("This Node is now the leader!");
        isLeader = true;
        globalConnectedPartitions = new HashMap<>();
      }
    }
  }

  private class DataFlowEventHandler implements FlowEventHandler {
    private int partitionId;
    public DataFlowEventHandler(int partitionId) {
      this.partitionId = partitionId;
    }
    @Override
    public void handleEvent(Object source, FlowEventArgs event) {
      flowEventHandler.handleEvent(source, event);

      if(event.getEvent() == FlowEvent.FLOW_ACTIVE) {
        logger.info("Connected to partition {}", this.partitionId);
        isLeader = true;
        globalConnectedPartitions = new HashMap<>();
      }
    }
  }

  private class MessageListener implements XMLMessageListener {
    @Override
    public void onException(JCSMPException exception) {
      listener.onException(exception);
    }

    @Override
    public void onReceive(BytesXMLMessage message) {
      listener.onReceive(message);
    }
  }

  private class CommandMessageListener implements XMLMessageListener {
    @Override
    public void onException(JCSMPException exception) {
      // TODO Auto-generated method stub
    }

    @Override
    public void onReceive(BytesXMLMessage message) {
      String destination = message.getDestination().getName();
      if (isLeader && destination.startsWith(TOPIC_PREFIX_STATE)) {
        String senderId = destination.substring(TOPIC_PREFIX_STATE.length());
        int[] senderConnectedPartitions = messageToPartitions(message);

        //logger.info("Received PQ-STATE command from {}: {}", senderId, senderConnectedPartitions);
        globalConnectedPartitions.put(senderId, senderConnectedPartitions);

        if (senderId.equals(instanceId)) {
          stateHoldCount++;
          if(stateHoldCount > 5) {
            requestRebalance();
          }
        }
        return;
      }

      if (destination.startsWith(TOPIC_PREFIX_REBALANCE)) {
        int[] partitionsToConnect = messageToPartitions(message);
        logger.info("Received REBALANCE command: {}", partitionsToConnect);
        performRebalance(partitionsToConnect);
        return;
      }
    }
  }

  private class CommandMessageProducer implements JCSMPStreamingPublishCorrelatingEventHandler {

    @Override
    public void handleErrorEx(Object key, JCSMPException cause, long timestamp) {
      // TODO Auto-generated method stub
      
    }

    @Override
    public void responseReceivedEx(Object key) {
      // TODO Auto-generated method stub
    }

  }

  private class StateProducerTimerTask extends TimerTask {
    @Override
    public void run() {
      BytesMessage message = partitionsToMessage(connectedPartitions);
      String topicString = TOPIC_PREFIX_STATE + instanceId;
      try {
        commandProducer.send(message, JCSMPFactory.onlyInstance().createTopic(topicString));
      } catch (JCSMPException e) {
        //TODO do anything?
      }
    }
  }
}
