package io.helidon.examples.coherence.scheduling;

import java.io.Serializable;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.oracle.coherence.common.base.Logger;
import com.oracle.coherence.concurrent.executor.ClusteredExecutorService;
import com.oracle.coherence.concurrent.executor.ClusteredOrchestration;
import com.oracle.coherence.concurrent.executor.RemoteExecutor;
import com.oracle.coherence.concurrent.executor.Task;
import com.oracle.coherence.concurrent.executor.options.Debugging;
import com.oracle.coherence.concurrent.executor.options.Member;
import com.oracle.coherence.concurrent.executor.options.Name;

/**
 * Scheduler.
 */
public final class Scheduler {

    private static final System.Logger LOGGER = System.getLogger(Scheduler.class.getName());

    private final ClusteredExecutorService executor = (ClusteredExecutorService) RemoteExecutor.get("virtual");
    private final Duration cancelTimeout = Duration.ofSeconds(10);

    /**
     * Serialized runnable that throws checked exceptions.
     */
    public interface Runnable extends Serializable {

        /**
         * Run.
         *
         * @throws Exception if an error occurs
         */
        void run() throws Exception;
    }

    /**
     * Schedule a task.
     *
     * @param id       task id
     * @param pattern  cron pattern
     * @param roles    member roles required
     * @param runnable runnable
     */
    public void schedule(String id, String pattern, List<String> roles, Runnable runnable) {
        CronTask<TaskExecution> task = new CronTask<>(ctx -> execute(ctx, runnable), pattern);

        // NOTE: work-around for a bug in NamedOrchestration.filter ??
        Task.Orchestration<TaskExecution> orchestration = new ClusteredOrchestration<>(executor, task)
                .as(id)
                .with(Debugging.of(Logger.FINEST))
                .limit(1) // only one execution per tick
                .filter(executorInfo -> {
                    Name name = executorInfo.getOption(Name.class, Name.UNNAMED);
                    if (!name.getName().equals("virtual")) {
                        // NOTE: work-around for a bug in NamedOrchestration.filter ??
                        return false;
                    }
                    Member memberOption = executorInfo.getOption(Member.class, null);
                    if (memberOption != null) {
                        return memberOption.get().getRoles().containsAll(roles);
                    }
                    return false;
                });

        try {
            Task.Coordinator<TaskExecution> coordinator = orchestration.submit();
            TaskFuture taskFuture = new TaskFuture();
            taskFuture.future().whenComplete((r, ex) -> Tasks.abortAll(id, ex));
            coordinator.subscribe(taskFuture);
        } catch (IllegalArgumentException ex) {
            LOGGER.log(Level.INFO, ex.getMessage(), ex);
        }
    }

    /**
     * Cancel a task.
     *
     * @param id task id
     * @return {@code true} if the task is known
     */
    public boolean cancel(String id) {
        Task.Coordinator<TaskExecution> coordinator = executor.acquire(id);
        if (coordinator == null) {
            return false;
        }
        LOGGER.log(Level.INFO, "Canceling task: {0}", id);
        TaskFuture taskFuture = new TaskFuture();
        coordinator.subscribe(taskFuture);
        coordinator.cancel(true);
        try {
            taskFuture.await(cancelTimeout);
        } catch (Exception ex) {
            LOGGER.log(Level.DEBUG, ex.getMessage(), ex);
        }
        return true;
    }

    /**
     * Execute a task.
     *
     * @param ctx      task context
     * @param runnable task runnable
     * @return TaskExecution
     */
    public static TaskExecution execute(Task.Context<TaskExecution> ctx, Runnable runnable) {
        String id = ctx.getTaskId();
        LOGGER.log(Level.INFO, () -> "Executing task: %s".formatted(id));
        TaskExecution execution = Tasks.execution(id);
        try {
            runnable.run();
            Tasks.complete(execution);
        } catch (Throwable e) {
            Tasks.abort(execution, e);
        }
        return execution;
    }

    /**
     * Task future.
     *
     * @param future future
     */
    record TaskFuture(CompletableFuture<Void> future) implements Task.Subscriber<TaskExecution> {

        TaskFuture() {
            this(new CompletableFuture<>());
        }

        /**
         * Wait for completion.
         *
         * @param timeout timeout
         * @throws Exception if an error occurs
         */
        void await(Duration timeout) throws Exception {
            future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        }

        @Override
        public void onComplete() {
            future.complete(null);
        }

        @Override
        public void onError(Throwable th) {
            future.completeExceptionally(th);
        }

        @Override
        public void onNext(TaskExecution execution) {
            // no-op
        }

        @Override
        public void onSubscribe(Task.Subscription<? extends TaskExecution> subscription) {
            // no-op
        }
    }
}
