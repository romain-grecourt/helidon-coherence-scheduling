## Helidon Coherence Scheduling Example

This project implements a distributed scheduler using Coherence and Helidon SE.

## Build and run

```shell
mvn package
java -Dcoherence.role="node1,scheduler" -Dserver.port=8081 -jar target/helidon-coherence-scheduling.jar &
sleep 5
java -Dcoherence.role="node2,scheduler" -jar target/helidon-coherence-scheduling.jar &
```

## Exercise the application

List the executions:
```shell
curl http://localhost:8080/scheduler/job-1 | jq -r .
```

Stop the first instance:
```shell
kill %1
```

Observe new executions on the 2nd instance (can take a few seconds):
```shell
curl http://localhost:8080/scheduler/job-1 | jq -r .
```

Cancel the job:
```shell
curl -X POST http://localhost:8080/scheduler/job-1/cancel
```

Observe that no new executions are added:
```shell
curl http://localhost:8080/scheduler/job-1 | jq -r .
```

Stop the 2nd instance:
```shell
kill %2
```
