package io.helidon.examples.coherence.scheduling;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import com.tangosol.io.ExternalizableLite;
import com.tangosol.util.ExternalizableHelper;

/**
 * Task execution.
 */
public final class TaskExecution implements ExternalizableLite {

    /**
     * State.
     */
    public enum State {
        RUNNING,
        COMPLETED,
        ABORTED
    }

    private State state;
    private String taskId;
    private String id;
    private Throwable error;
    private long timestamp;
    private int memberId;
    private Set<String> memberRoles;

    /**
     * Create a new instance.
     */
    @SuppressWarnings("unused")
    public TaskExecution() {
    }

    /**
     * Create a new instance.
     *
     * @param taskId      task id
     * @param memberId    member id
     * @param memberRoles member roles
     */
    public TaskExecution(String taskId, int memberId, Set<String> memberRoles) {
        this.id = UUID.randomUUID().toString();
        this.timestamp = Instant.now().truncatedTo(ChronoUnit.SECONDS).toEpochMilli();
        this.taskId = taskId;
        this.state = State.RUNNING;
        this.memberId = memberId;
        this.memberRoles = memberRoles;
    }

    /**
     * Complete this execution.
     */
    public void complete() {
        this.state = State.COMPLETED;
    }

    /**
     * Abort this execution.
     *
     * @param error error
     */
    public void abort(Throwable error) {
        this.state = State.ABORTED;
        this.error = error;
    }

    /**
     * Get the task id.
     *
     * @return task id
     */
    public String taskId() {
        return taskId;
    }

    /**
     * Get the execution id.
     *
     * @return execution id
     */
    public String id() {
        return id;
    }

    /**
     * Get the execution timestamp.
     *
     * @return timestamp
     */
    public long timestamp() {
        return timestamp;
    }

    /**
     * Get the state.
     *
     * @return state
     */
    public State state() {
        return state;
    }

    /**
     * Get the error.
     *
     * @return optional
     */
    public Optional<Throwable> error() {
        return Optional.ofNullable(error);
    }

    /**
     * Get the member id
     *
     * @return id
     */
    public int memberId() {
        return memberId;
    }

    /**
     * Get the member roles.
     *
     * @return roles
     */
    public Set<String> memberRoles() {
        return memberRoles;
    }

    @Override
    public void readExternal(DataInput in) throws IOException {
        taskId = ExternalizableHelper.readObject(in);
        id = ExternalizableHelper.readObject(in);
        timestamp = ExternalizableHelper.readLong(in);
        state = ExternalizableHelper.readObject(in);
        error = ExternalizableHelper.readObject(in);
        memberId = ExternalizableHelper.readInt(in);
        memberRoles = ExternalizableHelper.readObject(in);
    }

    @Override
    public void writeExternal(DataOutput out) throws IOException {
        ExternalizableHelper.writeObject(out, taskId);
        ExternalizableHelper.writeObject(out, id);
        ExternalizableHelper.writeLong(out, timestamp);
        ExternalizableHelper.writeObject(out, state);
        ExternalizableHelper.writeObject(out, error);
        ExternalizableHelper.writeInt(out, memberId);
        ExternalizableHelper.writeObject(out, memberRoles);
    }
}
