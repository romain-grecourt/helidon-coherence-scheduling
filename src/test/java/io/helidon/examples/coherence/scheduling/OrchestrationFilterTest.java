package io.helidon.examples.coherence.scheduling;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import com.oracle.coherence.concurrent.executor.ClusteredExecutorService;
import com.oracle.coherence.concurrent.executor.ClusteredOrchestration;
import com.oracle.coherence.concurrent.executor.RemoteExecutor;
import com.oracle.coherence.concurrent.executor.Task;
import com.oracle.coherence.concurrent.executor.Task.Subscriber;
import com.tangosol.net.Coherence;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.ExpectedToFail;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class OrchestrationFilterTest {

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
    @ExpectedToFail
    void testFilterIgnored() throws Exception {
        // NamedOrchestration.filter does not save the computed predicate
        doTest(executor -> executor.orchestrate(ctx -> true));
    }

    @Test
    void testFilterNotIgnored() throws Exception {
        doTest(executor -> new ClusteredOrchestration<>((ClusteredExecutorService) executor, ctx -> true));
    }

    void doTest(Function<RemoteExecutor, Task.Orchestration<Boolean>> factory) throws Exception {
        var executor = RemoteExecutor.getDefault();
        var future = new CompletableFuture<Void>();
        var orchestration = factory.apply(executor)
                .filter(executorInfo -> false)
                .subscribe(new Subscriber<>() {

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
        orchestration.submit();

        // we use a predicate that always returns false
        // so we expect a TimeoutException
        assertThrows(TimeoutException.class, () -> future.get(10, TimeUnit.SECONDS));
    }
}
