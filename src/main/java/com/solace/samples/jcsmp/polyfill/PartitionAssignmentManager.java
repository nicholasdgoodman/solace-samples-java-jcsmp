package com.solace.samples.jcsmp.polyfill;

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

public abstract class PartitionAssignmentManager {
  public static final String TOPIC_PREFIX_STATE = "#PQC/state";
  public static final String TOPIC_PREFIX_REBALANCE = "#PQC/rebalance";

  private static final int STATE_UPDATE_PERIOD = 1000;
  private static final int STATE_HOLD_COUNT = 5;

  protected String queueName;
  protected String instanceId;
  protected int partitionCount;
  private int stateHoldCount;

  private HashSet<Integer> assignedPartitions;
  private int[] connectedPartitions;
  private HashMap<String,int[]> globalConnectedPartitions;
  private Timer stateTimer;
  private boolean isLeader;

  public PartitionAssignmentManager(String queueName, int partitionCount) {
    this.queueName = queueName;
    this.instanceId = java.util.UUID.randomUUID().toString().substring(0,7);
    this.partitionCount = partitionCount;
    this.stateHoldCount = 0;

    this.assignedPartitions = new HashSet<>();
    this.connectedPartitions = new int[0];
    this.globalConnectedPartitions = new HashMap<>();
    this.stateTimer = new Timer();
  }

  public void start() {
    this.stateTimer.schedule(new StateProducerTimerTask(), STATE_UPDATE_PERIOD, STATE_UPDATE_PERIOD);
    this.addSubscriptions(
      String.join("/", TOPIC_PREFIX_REBALANCE, this.queueName, this.instanceId),
      String.join("/", TOPIC_PREFIX_STATE, this.queueName, ">" )
    );
  }

  public void setLeader(boolean isLeader) {
    this.isLeader = isLeader;
  }

  public void processCommand(String destination, ByteBuffer data) {
    if (destination.startsWith(TOPIC_PREFIX_STATE)) {
      this.processStateCommand(destination, data);
      return;
    }
    if (destination.startsWith(TOPIC_PREFIX_REBALANCE)) {
      this.processRebalanceCommand(data);
      return;
    }
  }

  public void processStateCommand(String destination, ByteBuffer data) {    
    String[] topicTokens = destination.split("/", 4);
    String senderId = (topicTokens.length == 4) ? topicTokens[3] : null;
    
    if(!this.isLeader || senderId == null) {
      return;
    }

    int[] senderPartitions = messageToPartitions(data);
    this.globalConnectedPartitions.put(senderId, senderPartitions);

    if (senderId.equals(instanceId)) {
      stateHoldCount++;
      if(stateHoldCount > STATE_HOLD_COUNT) {
        requestRebalance();
      }
    }
  }

  public void processRebalanceCommand(ByteBuffer data) {
    int[] partitionsToAssign = messageToPartitions(data);
    Set<Integer> newAssignments = Arrays.stream(partitionsToAssign).boxed().collect(Collectors.toSet());    
    HashSet<Integer> toConnect = new HashSet<>(newAssignments);
    toConnect.removeAll(this.assignedPartitions);
    HashSet<Integer> toDisconnect = new HashSet<>(this.assignedPartitions);
    toDisconnect.removeAll(newAssignments);

    this.assignedPartitions.clear();
    this.assignedPartitions.addAll(newAssignments);

    for(Integer partitionId : toDisconnect) {
      String partitionQueueName = String.join("/", 
        this.queueName, 
        Integer.toString(partitionId));

      this.disconnectFromPartition(partitionQueueName);
    }
    for(Integer partitionId : toConnect) {
      String partitionQueueName = String.join("/", 
        this.queueName, 
        Integer.toString(partitionId));

      this.connectToPartition(partitionQueueName);
    }
  }

  abstract void addSubscriptions(String stateTopic, String rebalanceTopic);
  abstract void sendCommand(String topic, byte[] data);
  abstract void connectToPartition(String partitionQueueName);
  abstract void disconnectFromPartition(String partitionQueueName);

  // TODO Rename to calculate assignments
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
      String topic = String.join("/", 
        TOPIC_PREFIX_REBALANCE, 
        this.queueName, 
        assignments.getConsumerId());
      byte[] data = partitionsToMessage(assignments.getPartitions());

      this.sendCommand(topic, data);
    }

    this.stateHoldCount = 0;
    this.globalConnectedPartitions.clear();
  }

  private static byte[] partitionsToMessage(int[] partitions) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(partitions.length * 4);        
    IntBuffer intBuffer = byteBuffer.asIntBuffer();
    intBuffer.put(partitions);
    return byteBuffer.array();
  }

  private static int[] messageToPartitions(ByteBuffer data) {
    IntBuffer intBuffer = data.asIntBuffer();
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

  private class StateProducerTimerTask extends TimerTask {
    @Override
    public void run() {
      String topicString = String.join("/", TOPIC_PREFIX_STATE, queueName, instanceId);
      byte[] message = partitionsToMessage(connectedPartitions);
      sendCommand(topicString, message);
    }
  }
}
