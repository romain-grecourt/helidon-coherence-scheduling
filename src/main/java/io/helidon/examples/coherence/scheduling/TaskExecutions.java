package io.helidon.examples.coherence.scheduling;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import com.tangosol.io.ExternalizableLite;
import com.tangosol.util.ExternalizableHelper;

/**
 * Task executions data.
 */
public class TaskExecutions implements Iterable<TaskExecution>, ExternalizableLite {

    private Map<String, TaskExecution> data = new LinkedHashMap<>();

    /**
     * Add a new execution.
     *
     * @param execution execution
     */
    public void add(TaskExecution execution) {
        data.put(execution.id(), execution);
    }

    /**
     * Complete an execution.
     *
     * @param executionId execution id
     */
    public void complete(String executionId) {
        TaskExecution execution = data.get(executionId);
        if (execution != null) {
            execution.complete();
        }
    }

    /**
     * Abort an execution.
     *
     * @param exception task exception
     */
    public void abort(TaskException exception) {
        TaskExecution execution = data.get(exception.executionId());
        if (execution != null) {
            execution.abort(exception.getCause());
        }
    }

    /**
     * Abort all running executions.
     *
     * @param error error
     */
    public void abortAll(Throwable error) {
        data.forEach((k, v) -> {
            if (v.state() == TaskExecution.State.RUNNING) {
                v.abort(error);
            }
        });
    }

    @Override
    public Iterator<TaskExecution> iterator() {
        return data.values().iterator();
    }

    @Override
    public void readExternal(DataInput in) throws IOException {
        data = ExternalizableHelper.readObject(in);
    }

    @Override
    public void writeExternal(DataOutput out) throws IOException {
        ExternalizableHelper.writeObject(out, data);
    }

}
