package io.helidon.examples.coherence.scheduling;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serial;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Objects;

import com.cronutils.model.Cron;
import com.cronutils.model.definition.CronDefinition;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import com.oracle.coherence.concurrent.executor.Task;
import com.tangosol.internal.util.invoke.Lambdas;
import com.tangosol.util.ExternalizableHelper;

import static com.cronutils.model.CronType.QUARTZ;

/**
 * A cron task that supports quartz expression.
 *
 * @param <T> task type
 */
public final class CronTask<T> extends com.oracle.coherence.concurrent.executor.tasks.CronTask<T> {

    private transient ExecutionTime executionTime;

    @SuppressWarnings("unused")
    public CronTask() {
    }

    /**
     * Create a new instance.
     *
     * @param task       task
     * @param expression expression
     */
    public CronTask(Task<T> task, String expression) {
        super(new TaskWrapper<>(task), expression);
        this.executionTime = parseCron();
    }

    @Override
    public long getNextExecutionMillis(long cMillis) {
        ZonedDateTime time = ZonedDateTime.ofInstant(Instant.ofEpochMilli(cMillis), ZoneOffset.UTC);
        if (executionTime == null) {
            executionTime = parseCron();
        }
        m_ldtNextExecutionMillis = executionTime.nextExecution(time)
                .map(ZonedDateTime::toInstant)
                .map(Instant::toEpochMilli)
                .orElseThrow();
        return m_ldtNextExecutionMillis;
    }

    private ExecutionTime parseCron() {
        CronDefinition cronDefinition = CronDefinitionBuilder.instanceDefinitionFor(QUARTZ);
        CronParser parser = new CronParser(cronDefinition);
        Cron cron = parser.parse(getCronPattern());
        return ExecutionTime.forCron(cron);
    }

    /**
     * NOTE: work-around for a bug in {@link CronTask#clone) ??
     */
    public static final class TaskWrapper<T> implements Task<T> {

        private Task<T> task;

        /**
         * Create a new instance.
         */
        @SuppressWarnings("unused")
        public TaskWrapper() {
        }

        /**
         * Create a new instance.
         *
         * @param task task
         * @throws NullPointerException if task is {@code null}
         */
        public TaskWrapper(Task<T> task) {
            Objects.requireNonNull(task, "task is null");
            this.task = Lambdas.ensureRemotable(task);
        }

        @Override
        public T execute(Context<T> context) throws Exception {
            return task.execute(context);
        }

        @Override
        public void readExternal(DataInput in) throws IOException {
            task = ExternalizableHelper.readObject(in);
        }

        @Override
        public void writeExternal(DataOutput out) throws IOException {
            ExternalizableHelper.writeObject(out, task);
        }

        @Serial
        private void readObject(ObjectInputStream in) throws IOException {
            readExternal(in);
        }

        @Serial
        private void writeObject(ObjectOutputStream out) throws IOException {
            writeExternal(out);
        }
    }
}
