package com.aliyun.openservices.aliyun.log.producer;

import com.aliyun.openservices.aliyun.log.producer.errors.ProducerException;
import com.aliyun.openservices.aliyun.log.producer.errors.ResultFailedException;
import com.aliyun.openservices.log.testing.IntegrationEnv;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ProducerMultiShardIT {

  @Rule public ExpectedException thrown = ExpectedException.none();

  @BeforeClass
  public static void loadEnv() {
    IntegrationEnv.loadOrSkip();
  }

  @Test
  public void testSend() throws InterruptedException, ProducerException, ExecutionException {
    ProducerConfig producerConfig = new ProducerConfig();
    final Producer producer = new LogProducer(producerConfig);
    producer.putProjectConfig(
        new ProjectConfig(
            System.getenv("PROJECT"),
            System.getenv("ENDPOINT"),
            System.getenv("ACCESS_KEY_ID"),
            System.getenv("ACCESS_KEY_SECRET")));
    producer.putProjectConfig(
        new ProjectConfig(
            System.getenv("OTHER_PROJECT"),
            System.getenv("ENDPOINT"),
            System.getenv("ACCESS_KEY_ID"),
            System.getenv("ACCESS_KEY_SECRET")));
    ListenableFuture<Result> f =
        producer.send(
            System.getenv("OTHER_PROJECT"),
            System.getenv("OTHER_LOG_STORE"),
            "",
            "shard3",
            "127.0.0.1",
            ProducerTestSupport.buildLogItem());
    Result result = f.get();
    Assert.assertTrue(result.isSuccessful());

    f =
        producer.send(
            System.getenv("OTHER_PROJECT"),
            System.getenv("OTHER_LOG_STORE"),
            null,
            "shard1",
            "192.168.0.2",
            ProducerTestSupport.buildLogItem());
    result = f.get();
    Assert.assertTrue(result.isSuccessful());

    producer.close();
    ProducerTestSupport.assertProducerFinalState(producer);
  }

  @Test
  public void testInvalidSend() throws InterruptedException, ProducerException {
    ProducerConfig producerConfig = new ProducerConfig();
    producerConfig.setAdjustShardHash(false);
    final Producer producer = new LogProducer(producerConfig);
    producer.putProjectConfig(
        new ProjectConfig(
            System.getenv("PROJECT"),
            System.getenv("ENDPOINT"),
            System.getenv("ACCESS_KEY_ID"),
            System.getenv("ACCESS_KEY_SECRET")));
    producer.putProjectConfig(
        new ProjectConfig(
            System.getenv("OTHER_PROJECT"),
            System.getenv("ENDPOINT"),
            System.getenv("ACCESS_KEY_ID"),
            System.getenv("ACCESS_KEY_SECRET")));
    ListenableFuture<Result> f =
        producer.send(
            System.getenv("OTHER_PROJECT"),
            System.getenv("OTHER_LOG_STORE"),
            "",
            "",
            "0",
            ProducerTestSupport.buildLogItem());
    try {
      f.get();
    } catch (ExecutionException e) {
      ResultFailedException resultFailedException = (ResultFailedException) e.getCause();
      Assert.assertEquals("ParameterInvalid", resultFailedException.getErrorCode());
    }
  }
}
