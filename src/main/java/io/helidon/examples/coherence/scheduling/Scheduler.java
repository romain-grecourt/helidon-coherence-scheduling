package io.helidon.examples.coherence.scheduling;

import java.io.Serializable;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.List;

import com.oracle.coherence.common.base.Logger;
import com.oracle.coherence.concurrent.executor.ClusteredExecutorService;
import com.oracle.coherence.concurrent.executor.RemoteExecutor;
import com.oracle.coherence.concurrent.executor.Task;
import com.oracle.coherence.concurrent.executor.options.Debugging;
import com.oracle.coherence.concurrent.executor.options.Member;

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

        Task.Orchestration<TaskExecution> orchestration = executor.orchestrate(task)
                .as(id)
                .with(Debugging.of(Logger.FINEST))
                .limit(1) // only one execution per tick
                .filter(executorInfo -> {
                    Member memberOption = executorInfo.getOption(Member.class, null);
                    if (memberOption != null) {
                        return memberOption.get().getRoles().containsAll(roles);
                    }
                    return false;
                });

        try {
            TaskFuture<TaskExecution> taskFuture = new TaskFuture<>();
            taskFuture.whenComplete((r, ex) -> Tasks.abortAll(id, ex));
            orchestration.subscribe(taskFuture).submit();
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
        TaskFuture<TaskExecution> taskFuture = new TaskFuture<>();
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

}
