package io.helidon.examples.coherence.scheduling;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import com.cronutils.model.Cron;
import com.cronutils.model.definition.CronDefinition;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import com.oracle.coherence.concurrent.executor.Task;

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
        super(task, expression, false);
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
}
