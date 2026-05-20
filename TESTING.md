# Testing aliyun-log-java-producer

Tests are split into two classes:

- **Unit tests** — class names end in `*Test`. Run by `mvn test` via
  `maven-surefire-plugin`. They never touch the network and are safe to run in
  any environment, including CI on every push.
- **Integration tests** — class names end in `*IT`. Run by `mvn verify` via
  `maven-failsafe-plugin`. They require a real Alibaba Cloud SLS endpoint and
  access keys. Without those, every IT is reported as *Skipped* (not Failed).

## Running unit tests

```bash
mvn -B test -Dgpg.skip=true
```

No environment variables required. All HTTP traffic in unit tests is intercepted
by `FakeServiceClient` injected via `LogProducer(ProducerConfig, ServiceClient)`.

## Running integration tests

Set credentials, then run `mvn verify`:

```bash
export LOG_TEST_ENDPOINT=cn-hangzhou.log.aliyuncs.com
export LOG_TEST_ACCESS_KEY_ID=...
export LOG_TEST_ACCESS_KEY_SECRET=...
export LOG_TEST_PROJECT=my-test-project
export LOG_TEST_LOG_STORE=my-test-logstore

mvn -B verify -Dgpg.skip=true
```

Legacy variable names (`ENDPOINT`, `ACCESS_KEY_ID`, `ACCESS_KEY_SECRET`,
`PROJECT`, `LOG_STORE`) are also accepted for backward compatibility.

When credentials are absent, `IntegrationEnv.loadOrSkip()` raises a JUnit
`Assume`, so each IT reports as *Skipped* rather than failing.

## Mock infrastructure (`src/test/java/com/aliyun/openservices/log/testing/`)

A small in-process toolkit for writing unit tests that exercise the HTTP
boundary without leaving the JVM.

### Injecting a mock transport

```java
import com.aliyun.openservices.log.testing.FakeServiceClient;
import com.aliyun.openservices.log.testing.RequestMatcher;
import com.aliyun.openservices.log.testing.Responses;
import com.aliyun.openservices.aliyun.log.producer.LogProducer;
import com.aliyun.openservices.aliyun.log.producer.ProducerConfig;
import com.aliyun.openservices.aliyun.log.producer.ProjectConfig;

FakeServiceClient fake = new FakeServiceClient();
fake.stub(RequestMatcher.method("POST").path("/logstores/my-ls/shards/lb"),
          Responses.ok(new byte[0]));

Producer producer = new LogProducer(new ProducerConfig(), fake);
producer.putProjectConfig(
    new ProjectConfig("my-project", "cn-mock.example.com", "mock-id", "mock-key"));

producer.send("my-project", "my-ls", logItem).get();

// Verify what the SDK actually sent.
assertEquals(1, fake.getReceivedRequests().size());
```

`FakeServiceClient` extends `com.aliyun.openservices.log.http.comm.ServiceClient`
and intercepts requests before they hit the network. The producer's full
batching, retry, and serialize pipeline runs end-to-end before the mock
transport returns a canned response.

`RequestMatcher` filters by HTTP method, path prefix, query parameters, and
headers. `Responses` builds canned `ResponseMessage` instances:

- `Responses.ok(byte[] body)` — 200 with raw bytes.
- `Responses.ok(String json)` — 200 with JSON body.
- `Responses.okProto(Message proto)` — 200 with serialized protobuf body.
- `Responses.error(int status, String code, String message)` — SLS error envelope.

### Mockito

`mockito-core` is on the test classpath for behavioral assertions ("called once
with X") rather than wire-level stubs. Prefer `FakeServiceClient` for
HTTP-shaped assertions; reach for Mockito for collaborator interactions.

### Skipping when no real endpoint is configured

Call `IntegrationEnv.loadOrSkip()` from `@BeforeClass`. When any required
credential variable is missing, the method raises `AssumptionViolatedException`
and JUnit reports the test as *Skipped*.

## Naming convention

- `*Test.java` — unit test, runs by default with surefire.
- `*IT.java` — integration test, runs only with `mvn verify` and requires
  real SLS credentials (otherwise skipped).

## CI

`.github/workflows/maven.yml` defines three jobs:

- `build` — `mvn package -DskipTests`, every push/PR.
- `unit-test` — `mvn test`, every push/PR. Network-free.
- `integration-test` — `mvn verify`, **manual only** (`workflow_dispatch`).
  Consumes `LOG_TEST_*` from repository secrets.

## Testing toolkit reuse

The `com.aliyun.openservices.log.testing` package is kept under
`src/test/java/` and intentionally duplicated here rather than imported from
`aliyun-log-java-sdk` via a `tests-jar` dependency. Update this copy in
lockstep when the java-sdk testing helpers change.
