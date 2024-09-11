package io.helidon.examples.coherence.scheduling;

import java.util.List;

import io.helidon.common.config.Config;
import io.helidon.common.config.GlobalConfig;
import io.helidon.common.media.type.MediaTypes;
import io.helidon.http.NotFoundException;
import io.helidon.webserver.http.HttpRules;
import io.helidon.webserver.http.HttpService;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;

import jakarta.json.Json;
import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonValue;
import jakarta.json.spi.JsonProvider;

/**
 * A simple HTTP service that schedules dummy jobs.
 */
final class SchedulerService implements HttpService {

    private static final JsonProvider JSON = JsonProvider.provider();

    private final Scheduler scheduler = new Scheduler();
    private final Tasks tasks = new Tasks();

    SchedulerService() {
        // schedule jobs on startup
        // only one member will do the scheduling, others will log an exception
        Config config = GlobalConfig.config();
        config.get("scheduler.jobs")
                .asNodeList()
                .orElseGet(List::of)
                .forEach(this::schedule);
    }

    @Override
    public void routing(HttpRules rules) {
        rules
                .get("/", this::listTasks)
                .get("/{id}", this::listExecutions)
                .post("/{id}/cancel", this::cancelTask);
    }

    private void schedule(Config config) {
        String id = config.key().name();
        String pattern = config.get("pattern").asString().get();
        // we use roles to illustrate a remote predicate
        List<String> roles = config.get("roles").asList(String.class).orElseGet(List::of);
        tasks.create(id);
        scheduler.schedule(id, pattern, roles, () -> {
            // sleep for 3 seconds to simulate a pseudo task
            Thread.sleep(3000);
        });
    }

    private void listTasks(ServerRequest req, ServerResponse res) {
        JsonArrayBuilder jsonBuilder = JSON.createArrayBuilder();
        tasks.forEach(jsonBuilder::add);
        res.headers().contentType(MediaTypes.APPLICATION_JSON);
        res.send(jsonBuilder.build());
    }

    private void listExecutions(ServerRequest req, ServerResponse res) {
        String taskId = req.path().pathParameters().get("id");
        JsonArrayBuilder arrayBuilder = JSON.createArrayBuilder();
        tasks.executions(taskId).orElseThrow(() -> new TaskNotFoundException(taskId))
                .forEach(execution -> arrayBuilder.add(JSON.createObjectBuilder()
                        .add("state", execution.state().name())
                        .add("timestamp", execution.timestamp())
                        .add("error", execution.error()
                                .map(ex -> (JsonValue) Json.createValue(ex.getMessage()))
                                .orElse(JsonValue.NULL))
                        .add("member-id", execution.memberId())
                        .add("member-roles", JSON.createArrayBuilder(execution.memberRoles()))));
        res.headers().contentType(MediaTypes.APPLICATION_JSON);
        res.send(arrayBuilder.build());
    }

    private void cancelTask(ServerRequest req, ServerResponse res) {
        String taskId = req.path().pathParameters().get("id");
        if (!scheduler.cancel(taskId)) {
            throw new TaskNotFoundException(taskId);
        }
        res.send();
    }

    private static final class TaskNotFoundException extends NotFoundException {

        public TaskNotFoundException(String taskId) {
            super("task %s not found".formatted(taskId));
        }
    }
}
