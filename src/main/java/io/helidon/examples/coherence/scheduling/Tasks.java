package io.helidon.examples.coherence.scheduling;

import java.util.Iterator;
import java.util.Optional;

import com.tangosol.net.CacheFactory;
import com.tangosol.net.Coherence;
import com.tangosol.net.Member;
import com.tangosol.net.NamedMap;

/**
 * Tasks data.
 */
public class Tasks implements Iterable<String> {

    private final NamedMap<String, TaskExecutions> data;

    /**
     * Create a new instance.
     */
    public Tasks() {
        data = CacheFactory.getCache("tasks");
    }

    /**
     * Create a new execution.
     *
     * @param taskId task id
     */
    public static TaskExecution execution(String taskId) {
        Member member = Coherence.getInstance().getCluster().getLocalMember();
        TaskExecution exec = new TaskExecution(taskId, member.getId(), member.getRoles());
        NamedMap<String, TaskExecutions> data = Coherence.getInstance().getSession().getMap("tasks");
        return data.invoke(taskId, entry -> {
            entry.update(TaskExecutions::add, exec);
            return exec;
        });
    }

    /**
     * Abort all running executions of a task.
     *
     * @param execution execution
     */
    public static void complete(TaskExecution execution) {
        NamedMap<String, TaskExecutions> data = Coherence.getInstance().getSession().getMap("tasks");
        data.invoke(execution.taskId(), entry -> {
            entry.update(TaskExecutions::complete, execution.id());
            return null;
        });
    }

    /**
     * Abort an execution.
     *
     * @param execution execution
     * @param error     error
     */
    public static void abort(TaskExecution execution, Throwable error) {
        NamedMap<String, TaskExecutions> data = Coherence.getInstance().getSession().getMap("tasks");
        data.invoke(execution.taskId(), entry -> {
            entry.update(TaskExecutions::abort, new TaskException(execution.taskId(), error));
            return null;
        });
    }

    /**
     * Abort all running executions of a task.
     *
     * @param taskId task id
     * @param error  error, may be {@code null}
     */
    public static void abortAll(String taskId, Throwable error) {
        NamedMap<String, TaskExecutions> data = Coherence.getInstance().getSession().getMap("tasks");
        data.invoke(taskId, entry -> {
            entry.update(TaskExecutions::abortAll, error);
            return null;
        });
    }

    /**
     * Initialize the executions for a new task.
     *
     * @param taskId task id
     */
    public void create(String taskId) {
        data.invoke(taskId, entry -> {
            if (entry.getValue() == null) {
                entry.setValue(new TaskExecutions());
            }
            return null;
        });
    }

    /**
     * Get the executions of a task.
     *
     * @param taskId task id
     * @return optional
     */
    public Optional<TaskExecutions> executions(String taskId) {
        return Optional.ofNullable(data.get(taskId));
    }

    @Override
    public Iterator<String> iterator() {
        return data.keySet().iterator();
    }
}
