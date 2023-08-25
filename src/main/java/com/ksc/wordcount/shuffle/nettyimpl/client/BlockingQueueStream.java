package com.ksc.wordcount.shuffle.nettyimpl.client;


import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

class BlockingQueueStream<T> {

    private BlockingQueue<T> queue;
    private boolean done = false;

    public BlockingQueueStream(int capacity) {
        queue = new LinkedBlockingQueue<T>(capacity);
    }


    public Stream<T> stream() {
        Spliterator<T> spliterator = new Spliterators.AbstractSpliterator<T>(Long.MAX_VALUE, Spliterator.ORDERED) {

            public boolean tryAdvance(Consumer<? super T> action) {
                while (true) {
                    if (done && queue.isEmpty()) {
                        return false;
                    }
                    T t = queue.poll();
//                    T t = queue.take();
                    if (t != null) {
                        action.accept(t);
                        return true;
                    }
                }
            }
        };
        return StreamSupport.stream(spliterator, false);
    }

    public void add(T t) {
//        queue.offer(t);
        try {
            queue.put(t);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void done() {
        done = true;
    }
}
