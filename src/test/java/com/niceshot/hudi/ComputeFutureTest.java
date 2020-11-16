package com.niceshot.hudi;

import lombok.SneakyThrows;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * @author created by chenjun at 2020-11-16 14:57
 */
public class ComputeFutureTest {
    @Test
    public void test() throws InterruptedException, ExecutionException {
        CompletableFuture<Void> hello = CompletableFuture.runAsync(new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
                Thread.sleep(5000l);
                System.out.println("hello");
            }
        });


        CompletableFuture<Void> world = CompletableFuture.runAsync(new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
                Thread.sleep(5000l);
                System.out.println("world");
            }
        });

        CompletableFuture.allOf(hello, world).get();
        System.out.println("finally");
    }
}
