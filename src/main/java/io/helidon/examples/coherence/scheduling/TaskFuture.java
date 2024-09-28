package io.helidon.examples.coherence.scheduling;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import com.oracle.coherence.concurrent.executor.Task;

/**
 * Task future.
 */
class TaskFuture<T> implements Task.Subscriber<T> {

    private final CompletableFuture<T> future = new CompletableFuture<>();
    private final AtomicReference<T> last = new AtomicReference<>();

    /**
     * Wait for completion.
     *
     * @param timeout timeout
     * @return T last item
     * @throws Exception if an error occurs
     */
    T await(Duration timeout) throws Exception {
        return future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * When complete callback.
     *
     * @param consumer callback
     */
    void whenComplete(BiConsumer<T, Throwable> consumer) {
        future.whenComplete(consumer);
    }

    @Override
    public void onComplete() {
        future.complete(last.get());
    }

    @Override
    public void onError(Throwable th) {
        future.completeExceptionally(th);
    }

    @Override
    public void onNext(T execution) {
        last.set(execution);
    }

    @Override
    public void onSubscribe(Task.Subscription<? extends T> subscription) {
        // no-op
    }
}
