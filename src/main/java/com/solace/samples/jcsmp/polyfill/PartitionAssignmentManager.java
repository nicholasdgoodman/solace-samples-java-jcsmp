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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.util.stream.Collectors;

public abstract class PartitionAssignmentManager {
  public static final String TOPIC_PREFIX_STATE = "#PQC/state";
  public static final String TOPIC_PREFIX_REBALANCE = "#PQC/rebalance";

  private static final Logger LOGGER = LogManager.getLogger(PartitionAssignmentManager.class);

  private static final int STATE_UPDATE_PERIOD = 1000;
  private static final int STATE_HOLD_COUNT = 5;

  private final Object lock = new Object();

  protected final String routerName;
  protected final String vpnName;
  protected final String queueName;
  protected final String instanceId;
  protected final int partitionCount;
  private final HashSet<Integer> assignedPartitions;
  private final HashMap<String,int[]> globalConnectedPartitions;
  
  private int stateHoldCount;
  private Timer stateTimer;
  private boolean isLeader;

  public PartitionAssignmentManager(String routerName, String vpnName, String queueName, String instanceId, int partitionCount) {
    this.routerName = routerName;
    this.vpnName = vpnName;
    this.queueName = queueName;
    this.instanceId = instanceId;
    this.partitionCount = partitionCount;
    this.assignedPartitions = new HashSet<>();
    this.globalConnectedPartitions = new HashMap<>();
    
    this.stateHoldCount = 0;
    this.stateTimer = new Timer();
  }

  public void start() {
    LOGGER.info("Starting PartitionAssignmentManager, instanceId = '{}'", instanceId);
    this.stateTimer.schedule(new StateProducerTimerTask(), STATE_UPDATE_PERIOD, STATE_UPDATE_PERIOD);
    this.addSubscriptions(
      String.join("/", TOPIC_PREFIX_REBALANCE, this.routerName, this.vpnName, this.queueName, this.instanceId),
      String.join("/", TOPIC_PREFIX_STATE, this.routerName, this.vpnName, this.queueName, ">" )
    );
  }

  public void setLeader(boolean isLeader) {
    synchronized(lock) {
      this.isLeader = isLeader;
    }
  }

  public void processCommand(String destination, ByteBuffer data) {
    LOGGER.debug("Processing command: '{}'", destination);
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
    synchronized(lock) {
      String[] topicTokens = destination.split("/", 6);
      String senderId = (topicTokens.length == 6) ? topicTokens[5] : null;
      
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
  }

  public void processRebalanceCommand(ByteBuffer data) {
    synchronized(lock) {
      int[] partitionsToAssign = messageToPartitions(data);
      Set<Integer> newAssignments = Arrays.stream(partitionsToAssign).boxed().collect(Collectors.toSet());    
      HashSet<Integer> toConnect = new HashSet<>(newAssignments);
      toConnect.removeAll(this.assignedPartitions);
      HashSet<Integer> toDisconnect = new HashSet<>(this.assignedPartitions);
      toDisconnect.removeAll(newAssignments);

      this.assignedPartitions.clear();
      this.assignedPartitions.addAll(newAssignments);

      for(Integer partitionId : toDisconnect) {
        this.disconnectFromPartition(partitionId);
      }
      for(Integer partitionId : toConnect) {
        this.connectToPartition(partitionId);
      }
    }
  }

  abstract void addSubscriptions(String stateTopic, String rebalanceTopic);
  abstract void sendCommand(String topic, byte[] data);
  abstract void connectToPartition(Integer partitionId);
  abstract void disconnectFromPartition(Integer partitionId);

  protected String getPartitionQueueName(Integer partitionId) {
    return String.join("/", 
      this.queueName, 
      Integer.toString(partitionId));
  }

  private void requestRebalance() {
    LOGGER.info("Evaluating partition assignments for rebalance.");
    synchronized(lock) {
      String[] partitionToConsumer = new String[this.partitionCount];
      HashMap<String, PartitionAssignments> assignmentMap = new HashMap<>();
      PartitionAssignments unassignedPartitions = new PartitionAssignments(null);
      
      boolean rebalanceRequired = false;
  
      for (Entry<String, int[]> connectedPartitionEntry : this.globalConnectedPartitions.entrySet()) {
        String consumerId = connectedPartitionEntry.getKey();
        int[] partitionIds = connectedPartitionEntry.getValue();
        PartitionAssignments assignments = new PartitionAssignments(consumerId);
  
        for (int partitionId : partitionIds) {
          if (partitionToConsumer[partitionId] == null) {
            partitionToConsumer[partitionId] = consumerId;
            assignments.addPartition(partitionId);
          } else {
            LOGGER.warn("Previously assigned partition ({}) detected, force rebalance.", partitionId);
            rebalanceRequired = true;
          }
        }
        assignmentMap.put(consumerId, assignments);
      }
      for (int n = 0; n < partitionToConsumer.length; n++) {
        if (partitionToConsumer[n] == null) {
          unassignedPartitions.addPartition(n);
        }
      }
  
      PartitionAssignments[] orderedAssignments = assignmentMap.values().toArray(new PartitionAssignments[0]);
  
      while (true) {
        Arrays.sort(orderedAssignments);
        PartitionAssignments minAssigned = orderedAssignments[0];
        PartitionAssignments maxAssigned = orderedAssignments[orderedAssignments.length - 1];
  
        if (unassignedPartitions.getPartitionCount() == 0
            && (maxAssigned.getPartitionCount() - minAssigned.getPartitionCount() <= 1)) {
          break;
        }
        
        if (unassignedPartitions.getPartitionCount() > 0) {
          int unassignedPartitionId = unassignedPartitions.removePartition();
          LOGGER.info("Assigning partition {} to {}.", unassignedPartitionId, minAssigned.getConsumerId());
          minAssigned.addPartition(unassignedPartitionId);
        } else {
          int reassignedPartitionId = maxAssigned.removePartition();
          LOGGER.info("Reassigning partition {} from {} to {}",
            reassignedPartitionId, maxAssigned.getConsumerId(), minAssigned.getConsumerId());
          minAssigned.addPartition(reassignedPartitionId);
        }
  
        // assignments changed. force rebalance.
        rebalanceRequired = true;
      }
  
      if (rebalanceRequired) {
        LOGGER.info("Broadcasting partition new assignments.");
        for (PartitionAssignments assignments : orderedAssignments) {
          String topic = String.join("/",
              TOPIC_PREFIX_REBALANCE,
              this.routerName,
              this.vpnName,
              this.queueName,
              assignments.getConsumerId());
          byte[] data = partitionsToMessage(assignments.getPartitions());
  
          this.sendCommand(topic, data);
        }
      } else {
        LOGGER.debug("No reassignments required. Skipping rebalance.");
      }
  
      this.stateHoldCount = 0;
      this.globalConnectedPartitions.clear();
    }
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
      int sizeResult = size1 - size2;
    
      if(sizeResult != 0) {
        return sizeResult;
      }

      return this.getConsumerId().compareTo(that.getConsumerId());
    }
    @Override
    public String toString() {
      return MessageFormat.format("'{'{0}={1}'}'", this.consumerId, this.partitions);
    }
  }

  private class StateProducerTimerTask extends TimerTask {
    @Override
    public void run() {
      synchronized(lock) {
        String topicString = String.join("/", TOPIC_PREFIX_STATE, routerName, vpnName, queueName, instanceId);
        int[] partitions = assignedPartitions.stream().mapToInt(v->v).toArray();
        Arrays.sort(partitions);
        byte[] message = partitionsToMessage(partitions);
        sendCommand(topicString, message);
      }
    }
  }
}
