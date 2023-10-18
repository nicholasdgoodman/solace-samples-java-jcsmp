package com.solace.samples.jcsmp.utils;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

public class OrderedExecutorService implements ExecutorService {
  private final int nThreads;
  private final List<ExecutorService> executorServices;
  private final Random random;
  private final MessageDigest digest;

  private OrderedExecutorService(int nThreads) {
    this.nThreads = nThreads;
    this.executorServices = Stream
      .generate(() -> Executors.newSingleThreadExecutor())
      .limit(nThreads)
      .toList();
    this.random = new Random();
    MessageDigest sha256Digest = null;
    try {
        sha256Digest = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException ex) {
        //this.digest = null;
    }
    this.digest = sha256Digest;
  }

  public static OrderedExecutorService newFixedThreadPool(int nThreads) {
    return new OrderedExecutorService(nThreads);
  }

  public void execute(Runnable command, String key) {
    int threadId = getThreadId(key);
    ExecutorService executorService = executorServices.get(threadId);
    executorService.execute(command);
  }

  @Override
  public void execute(Runnable command) {
    int threadId = getRandomThreadId();
    executorServices.get(threadId).execute(command);
  }

  @Override
  public void shutdown() {
    executorServices.stream().forEach(es -> es.shutdown());
  }

  @Override
  public List<Runnable> shutdownNow() {
    return executorServices.stream()
      .<Runnable>mapMulti((es, consumer) -> {
        for (Runnable pending : es.shutdownNow()) {
          consumer.accept(pending);
        }
    }).toList();
  }

  @Override
  public boolean isShutdown() {
    return executorServices.stream().allMatch(es -> es.isShutdown());
  }

  @Override
  public boolean isTerminated() {
    return executorServices.stream().allMatch(es -> es.isTerminated());
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    throw new UnsupportedOperationException("Unimplemented method 'awaitTermination'");
  }

  @Override
  public <T> Future<T> submit(Callable<T> task) {
    int threadId = getRandomThreadId();
    ExecutorService executorService = executorServices.get(threadId);
    return executorService.submit(task);
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    int threadId = getRandomThreadId();
    ExecutorService executorService = executorServices.get(threadId);
    return executorService.submit(task, result);
  }

  @Override
  public Future<?> submit(Runnable task) {
    int threadId = getRandomThreadId();
    ExecutorService executorService = executorServices.get(threadId);
    return executorService.submit(task);
  }

  public <T> Future<T> submit(Callable<T> task, String key) {
    int threadId = getThreadId(key);
    ExecutorService executorService = executorServices.get(threadId);
    return executorService.submit(task);
  }

  public <T> Future<T> submit(Runnable task, T result, String key) {
    int threadId = getThreadId(key);
    ExecutorService executorService = executorServices.get(threadId);
    return executorService.submit(task, result);
  }

  public Future<?> submit(Runnable task, String key) {
    int threadId = getThreadId(key);
    ExecutorService executorService = executorServices.get(threadId);
    return executorService.submit(task);
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'invokeAll'");
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'invokeAll'");
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'invokeAny'");
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'invokeAny'");
  }

  private int getThreadId(String key) {
    // modify the thread key to avoid moduluar interference from partition ID
    String formattedKey = String.format("/%s/", key);
    byte[] hashBytes = digest.digest(formattedKey.getBytes(StandardCharsets.UTF_8));
    int threadId = 
        (((hashBytes[0] & 0x7F) << 24) |
         ((hashBytes[1] & 0xFF) << 16) |
         ((hashBytes[2] & 0xFF) << 8 ) |
         ((hashBytes[3] & 0xFF) << 0 )) % nThreads;
    return threadId;
  }
  private int getRandomThreadId() {
    return random.nextInt(nThreads);
  }
}
