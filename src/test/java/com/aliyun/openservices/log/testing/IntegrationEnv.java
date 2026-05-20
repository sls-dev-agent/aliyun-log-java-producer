package com.aliyun.openservices.log.testing;

import org.junit.Assume;

/**
 * Resolves the SLS endpoint / credentials / project / logstore that the producer integration tests
 * need at runtime.
 *
 * <p>Each value is read from the new {@code LOG_TEST_*} variable first and falls back to the
 * historical {@code PROJECT} / {@code LOG_STORE} / {@code ENDPOINT} / {@code ACCESS_KEY_ID} /
 * {@code ACCESS_KEY_SECRET} variables that {@code ProducerTest} originally consumed. This lets us
 * keep existing CI / scratch setups working while the new {@code LOG_TEST_*} convention rolls out
 * across the SLS Java SDK family.
 *
 * <p>{@link #loadOrSkip()} is the primary entry point: when nothing is configured it raises a JUnit
 * {@link Assume#assumeTrue(String, boolean)} so failsafe reports the IT as <em>Skipped</em> instead
 * of an error. Call it from {@code @BeforeClass} (or {@code @Before}) — never from a static
 * initializer, which would surface the skip as an {@code ExceptionInInitializerError}.
 */
public final class IntegrationEnv {

  private static final String NEW_ENDPOINT = "LOG_TEST_ENDPOINT";
  private static final String NEW_ACCESS_KEY_ID = "LOG_TEST_ACCESS_KEY_ID";
  private static final String NEW_ACCESS_KEY_SECRET = "LOG_TEST_ACCESS_KEY_SECRET";
  private static final String NEW_PROJECT = "LOG_TEST_PROJECT";
  private static final String NEW_LOG_STORE = "LOG_TEST_LOG_STORE";

  private static final String LEGACY_ENDPOINT = "ENDPOINT";
  private static final String LEGACY_ACCESS_KEY_ID = "ACCESS_KEY_ID";
  private static final String LEGACY_ACCESS_KEY_SECRET = "ACCESS_KEY_SECRET";
  private static final String LEGACY_PROJECT = "PROJECT";
  private static final String LEGACY_LOG_STORE = "LOG_STORE";

  private final String endpoint;
  private final String accessKeyId;
  private final String accessKeySecret;
  private final String project;
  private final String logStore;

  private IntegrationEnv(
      String endpoint,
      String accessKeyId,
      String accessKeySecret,
      String project,
      String logStore) {
    this.endpoint = endpoint;
    this.accessKeyId = accessKeyId;
    this.accessKeySecret = accessKeySecret;
    this.project = project;
    this.logStore = logStore;
  }

  public String getEndpoint() {
    return endpoint;
  }

  public String getAccessKeyId() {
    return accessKeyId;
  }

  public String getAccessKeySecret() {
    return accessKeySecret;
  }

  public String getProject() {
    return project;
  }

  public String getLogStore() {
    return logStore;
  }

  /**
   * Resolve the test environment. Returns {@code null} if the four mandatory pieces (endpoint,
   * access key id, access key secret, project) are not all set; callers can {@code
   * Assume.assumeTrue} skip on null. The {@code logStore} is optional and may be {@code null}.
   */
  public static IntegrationEnv load() {
    String endpoint = first(NEW_ENDPOINT, LEGACY_ENDPOINT);
    String accessKeyId = first(NEW_ACCESS_KEY_ID, LEGACY_ACCESS_KEY_ID);
    String accessKeySecret = first(NEW_ACCESS_KEY_SECRET, LEGACY_ACCESS_KEY_SECRET);
    String project = first(NEW_PROJECT, LEGACY_PROJECT);
    String logStore = first(NEW_LOG_STORE, LEGACY_LOG_STORE);
    if (isBlank(endpoint) || isBlank(accessKeyId) || isBlank(accessKeySecret) || isBlank(project)) {
      return null;
    }
    return new IntegrationEnv(endpoint, accessKeyId, accessKeySecret, project, logStore);
  }

  /**
   * Like {@link #load()} but raises a JUnit {@link Assume} skip when no credentials are configured,
   * so the surrounding test is reported as <em>Skipped</em> instead of failing. Never returns
   * {@code null}.
   */
  public static IntegrationEnv loadOrSkip() {
    IntegrationEnv env = load();
    Assume.assumeTrue(
        "SKIP: requires "
            + NEW_ENDPOINT
            + "/"
            + NEW_ACCESS_KEY_ID
            + "/"
            + NEW_ACCESS_KEY_SECRET
            + "/"
            + NEW_PROJECT
            + " (or the legacy ENDPOINT/ACCESS_KEY_ID/ACCESS_KEY_SECRET/PROJECT) env vars",
        env != null);
    return env;
  }

  private static String first(String... keys) {
    for (String key : keys) {
      String value = System.getenv(key);
      if (!isBlank(value)) {
        return value;
      }
    }
    return null;
  }

  private static boolean isBlank(String s) {
    return s == null || s.isEmpty();
  }
}
