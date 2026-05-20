package com.aliyun.openservices.aliyun.log.producer;

import com.aliyun.openservices.log.common.LogItem;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;

/**
 * Static helpers shared between unit and integration tests of the LogProducer. Extracted from the
 * historical {@code ProducerTest} class so the renamed {@code ProducerIT} integration suite can be
 * skipped without breaking unit-only consumers (e.g. {@code internals/ProducerBatchTest}, {@code
 * internals/LogSizeCalculatorTest}, {@code ProducerInvalidIT}).
 */
public final class ProducerTestSupport {

  private ProducerTestSupport() {}

  /** Assert the producer drained all in-flight batches and released its memory budget. */
  public static void assertProducerFinalState(Producer producer) {
    Assert.assertEquals(0, producer.getBatchCount());
    Assert.assertEquals(
        producer.getProducerConfig().getTotalSizeInBytes(), producer.availableMemoryInBytes());
  }

  /** A two-field {@link LogItem} used as a generic, deterministic test payload. */
  public static LogItem buildLogItem() {
    LogItem logItem = new LogItem();
    logItem.PushBack("k1", "v1");
    logItem.PushBack("k2", "v2");
    return logItem;
  }

  /** A list of {@code n} identical log items, each produced via {@link #buildLogItem()}. */
  public static List<LogItem> buildLogItems(int n) {
    List<LogItem> logItems = new ArrayList<LogItem>();
    for (int i = 0; i < n; ++i) {
      logItems.add(buildLogItem());
    }
    return logItems;
  }
}
