package io.github.de314.ac.data.utils;

import java.util.Optional;

public class UncheckedException extends RuntimeException {

    public UncheckedException(Throwable t) {
        super(t);
    }

    public UncheckedException(String message) {
        super(message);
    }

    public UncheckedException(String message, Throwable t) {
        super(message, t);
    }

    public static void of(Throwable t) {
        throw new UncheckedException(t);
    }

    public static void of(String message) {
        throw new UncheckedException(message);
    }

    public static void of(String message, Throwable t) {
        throw new UncheckedException(message, t);
    }

    public static void safe(ExceptionalRunnable runnable) {
        try {
            runnable.run();
        } catch (Exception e) {
            UncheckedException.of(e);
        }
    }

    public static <T> Optional<T> safe(ExceptionalSupplier<T> supplier) {
        try {
            return Optional.ofNullable(supplier.get());
        } catch (Exception e) {
            UncheckedException.of(e);
        }
        return Optional.empty();
    }

    public interface ExceptionalRunnable {
        void run() throws Exception;
    }

    public interface ExceptionalSupplier<T> {
        T get() throws Exception;
    }
}
