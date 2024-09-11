package io.helidon.examples.coherence.scheduling;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serial;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import com.oracle.coherence.concurrent.Latches;
import com.oracle.coherence.concurrent.executor.RemoteExecutor;
import com.oracle.coherence.concurrent.executor.Task;
import com.oracle.coherence.concurrent.executor.tasks.CronTask;
import com.tangosol.net.Coherence;
import com.tangosol.util.ExternalizableHelper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.ExpectedToFail;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CronTaskTest {

    @BeforeAll
    @SuppressWarnings("resource")
    static void boostrapCoherence() throws InterruptedException {
        System.setProperty("coherence.wka", "127.0.0.1");
        Coherence coherence = Coherence.clusterMember();
        coherence.startAndWait();
    }

    @AfterAll
    static void shutdownCoherence() {
        Coherence coherence = Coherence.getInstance();
        coherence.close();
    }

    @Test
    void testConcrete() throws Exception {
        doTest(ConcreteTask::new, "concrete-task");
    }

    @Test
    @ExpectedToFail
    void testLambda() throws Exception {
        // After the 1st tick, the task is cloned but the result is null
        // Thus the 2nd tick throws a NPE
        doTest((s, i) -> ctx -> {
            Latches.remoteCountDownLatch(s, i).countDown();
            return true;
        }, "lambda-task");
    }

    @Test
    void testWrappedLambda() throws Exception {
        // workaround: wrap the lambda in a task that implements readObject and writeObject
        doTest((s, i) -> new TaskWrapper(ctx -> {
            Latches.remoteCountDownLatch(s, i).countDown();
            return true;
        }), "wrapped-lambda-task");
    }

    void doTest(BiFunction<String, Integer, Task<Boolean>> factory, String id) throws Exception {
        // use a latch to wait for 2 ticks
        var latchName = id + "-latch";
        var latch = Latches.remoteCountDownLatch(latchName, 2);

        var executor = RemoteExecutor.getDefault();
        var cronTask = new EverySecondTask(factory.apply(latchName, 2));
        var coordinator = executor.orchestrate(cronTask).as(id).submit();

        // use a future updated with a subscriber
        // to block for completion and raise errors
        var future = new CompletableFuture<Void>();
        coordinator.subscribe(new Task.Subscriber<>() {
            @Override
            public void onComplete() {
                future.complete(null);
            }

            @Override
            public void onError(Throwable throwable) {
                future.completeExceptionally(throwable);
            }

            @Override
            public void onNext(Boolean item) {
            }

            @Override
            public void onSubscribe(Task.Subscription<? extends Boolean> subscription) {
            }
        });

        // wait for 2 ticks
        latch.await(10, TimeUnit.SECONDS);

        // cancel the task
        coordinator.cancel(true);

        // we always expect an ExecutionException because cancellation throws an InterruptedException
        // the cause may be different if an error occurred before cancellation
        var ex = assertThrows(ExecutionException.class, () -> future.get(10, TimeUnit.SECONDS));
        if (!(ex.getCause() instanceof InterruptedException ie)) {
            throw new AssertionError(ex.getCause());
        }
        assertThat(ie.getMessage(), is("Task %s has been cancelled.".formatted(id)));
    }

    /**
     * A practical test cron task that runs every second.
     */
    public static class EverySecondTask extends CronTask<Boolean> {

        @SuppressWarnings("unused")
        public EverySecondTask() {
        }

        public EverySecondTask(Task<Boolean> task) {
            super(task, "* * * * *");
        }

        @Override
        public long getNextExecutionMillis(long cMillis) {
            // execute every second
            m_ldtNextExecutionMillis = cMillis + 1000;
            return m_ldtNextExecutionMillis;
        }
    }

    /**
     * A concrete task.
     */
    public static class ConcreteTask implements Task<Boolean> {

        private String latchName;
        private int latchCount;

        @SuppressWarnings("unused")
        public ConcreteTask() {
        }

        public ConcreteTask(String latchName, int latchCount) {
            this.latchName = latchName;
            this.latchCount = latchCount;
        }

        @Override
        public Boolean execute(Context<Boolean> context) {
            Latches.remoteCountDownLatch(latchName, 2).countDown();
            return true;
        }

        @Override
        public void readExternal(DataInput in) throws IOException {
            latchName = ExternalizableHelper.readObject(in);
            latchCount = ExternalizableHelper.readInt(in);
        }

        @Override
        public void writeExternal(DataOutput out) throws IOException {
            ExternalizableHelper.writeObject(out, latchName);
            ExternalizableHelper.writeInt(out, latchCount);
        }
    }

    /**
     * A task wrapper that implements {@code readObject} and {@code writeObject}.
     */
    public static final class TaskWrapper implements Task<Boolean> {

        private Task<Boolean> task;

        @SuppressWarnings("unused")
        public TaskWrapper() {
        }

        public TaskWrapper(Task<Boolean> task) {
            this.task = task;
        }

        @Override
        public Boolean execute(Context<Boolean> context) throws Exception {
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
