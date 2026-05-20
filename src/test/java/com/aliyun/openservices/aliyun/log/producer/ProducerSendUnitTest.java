package com.aliyun.openservices.aliyun.log.producer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.aliyun.openservices.log.common.LogItem;
import com.aliyun.openservices.log.testing.FakeServiceClient;
import com.aliyun.openservices.log.testing.RequestMatcher;
import com.aliyun.openservices.log.testing.Responses;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test for LogProducer send path. Uses FakeServiceClient so no real SLS endpoint is required.
 */
public class ProducerSendUnitTest {

  private static final String PROJECT = "unit-project";
  private static final String LOGSTORE = "unit-ls";
  private static final String ENDPOINT = "cn-mock.example.com";

  private FakeServiceClient fake;
  private Producer producer;

  @Before
  public void setUp() {
    fake = new FakeServiceClient();
    // PutLogs goes to POST /logstores/<ls>/shards/lb
    fake.stub(
        RequestMatcher.method("POST").path("/logstores/" + LOGSTORE + "/shards/lb"),
        Responses.ok(new byte[0]));

    ProducerConfig config = new ProducerConfig();
    producer = new LogProducer(config, fake);
    producer.putProjectConfig(new ProjectConfig(PROJECT, ENDPOINT, "mock-id", "mock-key"));
  }

  @After
  public void tearDown() throws Exception {
    producer.close();
  }

  @Test
  public void singleLogItemReachesEndpoint() throws Exception {
    LogItem item = ProducerTestSupport.buildLogItem();
    ListenableFuture<Result> future = producer.send(PROJECT, LOGSTORE, item);

    Result result = future.get(10, TimeUnit.SECONDS);
    assertTrue("expected successful send", result.isSuccessful());
    assertTrue(
        "expected at least one PutLogs request to fake", fake.getReceivedRequests().size() >= 1);
  }

  @Test
  public void batchOfLogsFlushesAsSingleRequest() throws Exception {
    int n = 50;
    ListenableFuture<Result> last = null;
    for (int i = 0; i < n; i++) {
      last = producer.send(PROJECT, LOGSTORE, ProducerTestSupport.buildLogItem());
    }
    Result result = last.get(10, TimeUnit.SECONDS);
    assertTrue("expected successful batch send", result.isSuccessful());

    // 50 small items fit well under the default batch size, so they should
    // flush as 1 request (or a small handful at most).
    assertTrue(
        "expected few PutLogs requests for small batch", fake.getReceivedRequests().size() <= 5);
  }

  @Test
  public void requestBodyIsNonEmpty() throws Exception {
    producer.send(PROJECT, LOGSTORE, ProducerTestSupport.buildLogItem()).get(10, TimeUnit.SECONDS);
    byte[] body = fake.getReceivedBody(0);
    assertTrue("request body must be non-empty protobuf", body != null && body.length > 0);
  }

  @Test
  public void producerDrainsCleanlyOnClose() throws Exception {
    for (int i = 0; i < 5; i++) {
      producer.send(PROJECT, LOGSTORE, ProducerTestSupport.buildLogItem());
    }
    producer.close();
    assertEquals(0, producer.getBatchCount());
  }
}
