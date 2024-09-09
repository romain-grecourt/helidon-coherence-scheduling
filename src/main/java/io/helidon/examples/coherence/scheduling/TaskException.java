package io.helidon.examples.coherence.scheduling;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.tangosol.io.ExternalizableLite;
import com.tangosol.util.ExternalizableHelper;

/**
 * Task exception.
 */
public final class TaskException extends Exception implements ExternalizableLite {

    private String executionId;

    /**
     * Create a new instance.
     */
    @SuppressWarnings("unused")
    public TaskException() {
    }

    /**
     * Create a new instance.
     *
     * @param executionId  execution id
     * @param cause cause
     */
    public TaskException(String executionId, Throwable cause) {
        super(cause);
        this.executionId = executionId;
    }

    /**
     * Get the execution id.
     *
     * @return execution id
     */
    public String executionId() {
        return executionId;
    }

    @Override
    public void readExternal(DataInput in) throws IOException {
        executionId = ExternalizableHelper.readObject(in);
    }

    @Override
    public void writeExternal(DataOutput out) throws IOException {
        ExternalizableHelper.writeObject(out, executionId);
    }
}
