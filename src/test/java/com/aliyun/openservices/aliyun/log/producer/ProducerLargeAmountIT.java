package com.aliyun.openservices.aliyun.log.producer;

import com.aliyun.openservices.aliyun.log.producer.errors.ProducerException;
import com.aliyun.openservices.log.common.LogItem;
import com.aliyun.openservices.log.testing.IntegrationEnv;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class ProducerLargeAmountIT {

  private static IntegrationEnv ENV;

  @BeforeClass
  public static void loadEnv() {
    ENV = IntegrationEnv.loadOrSkip();
  }

  private final Random random = new Random();

  @Test
  public void testSend() throws InterruptedException, ProducerException {
    ProducerConfig producerConfig = new ProducerConfig();
    final Producer producer = new LogProducer(producerConfig);
    producer.putProjectConfig(buildProjectConfig());
    final AtomicInteger successCount = new AtomicInteger(0);
    final int nTasks = 100;
    final int times = 10000;
    ExecutorService executorService = Executors.newFixedThreadPool(nTasks);
    final CountDownLatch latch = new CountDownLatch(nTasks);
    for (int i = 0; i < nTasks; ++i) {
      executorService.submit(
          new Runnable() {
            @Override
            public void run() {
              try {
                for (int i = 0; i < times; ++i) {
                  producer.send(
                      ENV.getProject(),
                      ENV.getLogStore(),
                      getTopic(),
                      getSource(),
                      ProducerTestSupport.buildLogItem(),
                      new Callback() {
                        @Override
                        public void onCompletion(Result result) {
                          if (result.isSuccessful()) {
                            successCount.incrementAndGet();
                          }
                        }
                      });
                }
              } catch (Exception e) {
                e.printStackTrace();
              }
              latch.countDown();
            }
          });
    }
    latch.await();
    executorService.shutdown();
    Thread.sleep(producerConfig.getLingerMs() * 2);
    producer.close();
    Assert.assertEquals(nTasks * times, successCount.get());
    ProducerTestSupport.assertProducerFinalState(producer);
  }

  @Test
  public void testSendLogs() throws InterruptedException, ProducerException {
    ProducerConfig producerConfig = new ProducerConfig();
    final Producer producer = new LogProducer(producerConfig);
    final AtomicInteger successCount = new AtomicInteger(0);
    final int nTasks = 100;
    final int times = 1000;
    final List<LogItem> logItems = ProducerTestSupport.buildLogItems(50);
    ExecutorService executorService = Executors.newFixedThreadPool(nTasks);
    final CountDownLatch latch = new CountDownLatch(nTasks);
    for (int i = 0; i < nTasks; ++i) {
      executorService.submit(
          new Runnable() {
            @Override
            public void run() {
              try {
                for (int i = 0; i < times; ++i) {
                  producer.send(
                      ENV.getProject(),
                      ENV.getLogStore(),
                      getTopic(),
                      getSource(),
                      logItems,
                      new Callback() {
                        @Override
                        public void onCompletion(Result result) {
                          if (result.isSuccessful()) {
                            successCount.incrementAndGet();
                          }
                        }
                      });
                }
              } catch (Exception e) {
                e.printStackTrace();
              }
              latch.countDown();
            }
          });
    }
    latch.await();
    executorService.shutdown();
    Thread.sleep(producerConfig.getLingerMs() * 4);
    producer.close();
    Assert.assertEquals(nTasks * times, successCount.get());
    ProducerTestSupport.assertProducerFinalState(producer);
  }

  private ProjectConfig buildProjectConfig() {
    return new ProjectConfig(
        ENV.getProject(), ENV.getEndpoint(), ENV.getAccessKeyId(), ENV.getAccessKeySecret());
  }

  private String getTopic() {
    return "topic-" + random.nextInt(5);
  }

  private String getSource() {
    return "source-" + random.nextInt(10);
  }
}
