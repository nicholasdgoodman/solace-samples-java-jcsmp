package com.solace.samples.jcsmp.polyfill;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.SDTException;
import com.solacesystems.jcsmp.SDTMap;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessage;
import com.solacesystems.jcsmp.XMLMessageProducer;

public class PartitionedMessageProducer {
  public static final String QUEUE_PARTITION_KEY = "JMSXGroupId";

  private XMLMessageProducer producer;
  private int partitionCount;
  private Random rand;
  private MessageDigest digest;

  private PartitionedMessageProducer(XMLMessageProducer producer, int partitionCount) {
    this.producer = producer;
    this.partitionCount = partitionCount;
    this.rand = new Random();
    try {
        this.digest = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException ex) {
        // we know SHA-256 is valid.. do nothing!
    }
  }

  public static PartitionedMessageProducer from(XMLMessageProducer producer, int partitionCount) {
    return new PartitionedMessageProducer(producer, partitionCount);
  }

  public void send(XMLMessage message, Topic topic) throws JCSMPException {
    this.producer.send(message, this.partitionTopic(message, topic));
  }
  
  private Topic partitionTopic(XMLMessage message, Topic topic) {
    String partitionId = getPartitionId(message);
    String topicString = String.join("/", topic.getName(), partitionId);
    return JCSMPFactory.onlyInstance().createTopic(topicString);
  }
  private String getPartitionId(XMLMessage message) {
    try {
      SDTMap map = message.getProperties();
      if(map == null) {
        return getRandomPartitionId();
      }
      String partitionKey = map.getString(QUEUE_PARTITION_KEY);
      if(partitionKey == null) {
        return getRandomPartitionId();
      }
      return getPartitionIdFromKey(partitionKey);
    } catch (SDTException e) {
      return getRandomPartitionId();
    }
  }
  private String getPartitionIdFromKey(String partitionKey) {
    byte[] hashBytes = digest.digest(partitionKey.getBytes(StandardCharsets.UTF_8));
    int partitionId = 
        (((hashBytes[0] & 0x7F) << 24) |
         ((hashBytes[1] & 0xFF) << 16) |
         ((hashBytes[2] & 0xFF) << 8 ) |
         ((hashBytes[3] & 0xFF) << 0 )) % partitionCount;
    return String.valueOf(partitionId);
  }
  private String getRandomPartitionId() {
    return Integer.toString(this.rand.nextInt() % this.partitionCount);
  }
}
