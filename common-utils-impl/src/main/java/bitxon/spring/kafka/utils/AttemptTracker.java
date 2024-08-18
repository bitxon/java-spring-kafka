package bitxon.spring.kafka.utils;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class AttemptTracker<T> {
    private final AtomicInteger counter = new AtomicInteger();
    private final ConcurrentHashMap<Integer, T> attempt = new ConcurrentHashMap<>();

    public void put(T value) {
        attempt.put(counter.incrementAndGet(), value);
    }

    public ConcurrentHashMap<Integer, T> getAll() {
        return new ConcurrentHashMap<>(attempt);
    }

    public void clear() {
        counter.set(0);
        attempt.clear();
    }
}
