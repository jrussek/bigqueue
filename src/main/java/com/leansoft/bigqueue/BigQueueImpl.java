package com.leansoft.bigqueue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.leansoft.bigqueue.page.IMappedPage;
import com.leansoft.bigqueue.page.IMappedPageFactory;
import com.leansoft.bigqueue.page.MappedPageFactoryImpl;


/**
 * A big, fast and persistent queue implementation.
 * <p/>
 * Main features:
 * 1. FAST : close to the speed of direct memory access, both enqueue and dequeue are close to O(1) memory access.
 * 2. MEMORY-EFFICIENT : automatic paging & swapping algorithm, only most-recently accessed data is kept in memory.
 * 3. THREAD-SAFE : multiple threads can concurrently enqueue and dequeue without data corruption.
 * 4. PERSISTENT - all data in queue is persisted on disk, and is crash resistant.
 * 5. BIG(HUGE) - the total size of the queued data is only limited by the available disk space.
 *
 * @author bulldog
 */
public class BigQueueImpl implements IBigQueue {

    final IBigArray innerArray;

    // 2 ^ 3 = 8
    final static int QUEUE_FRONT_INDEX_ITEM_LENGTH_BITS = 3;
    // size in bytes of queue front index page
    final static int QUEUE_FRONT_INDEX_PAGE_SIZE = 1 << QUEUE_FRONT_INDEX_ITEM_LENGTH_BITS;
    // only use the first page
    static final long QUEUE_FRONT_PAGE_INDEX = 0;

    // folder name for queue front index page
    final static String QUEUE_FRONT_INDEX_PAGE_FOLDER = "front_index";

    // front index of the big queue,
    final AtomicLong queueFrontIndex = new AtomicLong();

    // factory for queue front index page management(acquire, release, cache)
    IMappedPageFactory queueFrontIndexPageFactory;

    // locks for queue front write management
    final Lock queueFrontWriteLock = new ReentrantLock();

    // lock for dequeueFuture access
    private final Object futureLock = new Object();

    private int maximumOutstandingRequests = 10;

    private static Queue<FutureRequest> futureQueue;

    private volatile int batchSize = 0;

    private class FutureRequest {
        private int requestedBatchsize;
        private SettableFuture<byte[][]> resultFuture;

        public FutureRequest(int requestedBatchsize, SettableFuture<byte[][]> resultFuture) {
            this.requestedBatchsize = requestedBatchsize;
            this.resultFuture = resultFuture;
        }

        public FutureRequest(SettableFuture<byte[][]> resultFuture) {
            this.requestedBatchsize = 1;
            this.resultFuture = resultFuture;
        }

        public int getRequestedBatchsize() {
            return requestedBatchsize;
        }

        public SettableFuture<byte[][]> getResultFuture() {
            return resultFuture;
        }
    }

    /**
     * A big, fast and persistent queue implementation,
     * use default back data page size, see {@link BigArrayImpl#DEFAULT_DATA_PAGE_SIZE}
     *
     * @param queueDir  the directory to store queue data
     * @param queueName the name of the queue, will be appended as last part of the queue directory
     * @throws IOException exception throws if there is any IO error during queue initialization
     */
    public BigQueueImpl(String queueDir, String queueName) throws IOException {
        this(queueDir, queueName, BigArrayImpl.DEFAULT_DATA_PAGE_SIZE);
    }

    /**
     * A big, fast and persistent queue implementation.
     *
     * @param queueDir  the directory to store queue data
     * @param queueName the name of the queue, will be appended as last part of the queue directory
     * @param pageSize  the back data file size per page in bytes, see minimum allowed {@link BigArrayImpl#MINIMUM_DATA_PAGE_SIZE}
     * @throws IOException exception throws if there is any IO error during queue initialization
     */
    public BigQueueImpl(String queueDir, String queueName, int pageSize) throws IOException {
        if(futureQueue == null) {
            futureQueue = new LinkedBlockingDeque<FutureRequest>(maximumOutstandingRequests);
        }
        innerArray = new BigArrayImpl(queueDir, queueName, pageSize);

        // the ttl does not matter here since queue front index page is always cached
        this.queueFrontIndexPageFactory = new MappedPageFactoryImpl(QUEUE_FRONT_INDEX_PAGE_SIZE,
                ((BigArrayImpl) innerArray).getArrayDirectory() + QUEUE_FRONT_INDEX_PAGE_FOLDER,
                10 * 1000/*does not matter*/);
        IMappedPage queueFrontIndexPage = this.queueFrontIndexPageFactory.acquirePage(QUEUE_FRONT_PAGE_INDEX);

        ByteBuffer queueFrontIndexBuffer = queueFrontIndexPage.getLocal(0);
        long front = queueFrontIndexBuffer.getLong();
        queueFrontIndex.set(front);
    }

    @Override
    public boolean isEmpty() {
        return this.queueFrontIndex.get() == this.innerArray.getHeadIndex();
    }

    @Override
    public void enqueue(byte[] data) throws IOException {
        this.innerArray.append(data);
        synchronized (futureLock) {
            if (this.batchSize > 0 && this.size() >= this.batchSize) {
                FutureRequest request = this.futureQueue.poll();
                while(request != null) {
                    SettableFuture<byte[][]> future = request.getResultFuture();
                    if(future.isDone()) {
                        request = this.futureQueue.poll();
                    } else {
                        future.set(this.dequeue(this.batchSize));
                        break;
                    }
                }
                if (this.futureQueue.isEmpty()) {
                    this.batchSize = 0;
                } else {
                    this.batchSize = this.futureQueue.peek().getRequestedBatchsize();
                }
            }
        }
    }


    @Override
    public byte[] dequeue() throws IOException {
        if(this.isEmpty()) {
            return null;
        }
        try {
            return dequeueAsync().get();
        } catch (InterruptedException e) {
            throw new IOException(e.getMessage());
        } catch (ExecutionException e) {
            throw new IOException(e.getMessage());
        }
    }

    @Override
    public byte[][] dequeue(int n) throws IOException {
        byte[][] buffer = new byte[n][];
        long queueFrontIndex = -1L;
        try {
            queueFrontWriteLock.lock();
            if (this.isEmpty()) {
                return null;
            }
            queueFrontIndex = this.queueFrontIndex.get();
            for(int i=0; i<n && queueFrontIndex+i < this.innerArray.getHeadIndex(); i++) {
                buffer[i] = this.innerArray.get(queueFrontIndex+i);
            }
            long nextQueueFrontIndex = queueFrontIndex;
            if (nextQueueFrontIndex >= Long.MAX_VALUE - n) {
                nextQueueFrontIndex = n - (Long.MAX_VALUE - nextQueueFrontIndex); // wrap
            } else {
                nextQueueFrontIndex+=n;
            }
            this.queueFrontIndex.set(nextQueueFrontIndex);
            // persist the queue front
            IMappedPage queueFrontIndexPage = this.queueFrontIndexPageFactory.acquirePage(QUEUE_FRONT_PAGE_INDEX);
            ByteBuffer queueFrontIndexBuffer = queueFrontIndexPage.getLocal(0);
            queueFrontIndexBuffer.putLong(nextQueueFrontIndex);
            queueFrontIndexPage.setDirty(true);
        } finally {
            queueFrontWriteLock.unlock();
        }
        return buffer;
    }

    @Override
    public ListenableFuture<byte[]> dequeueAsync() {
        return Futures.transform(this.dequeueAsync(1), new AsyncFunction<byte[][], byte[]>() {
            @Override
            public ListenableFuture<byte[]> apply(byte[][] bytes) throws Exception {
                if(bytes != null) {
                    return Futures.immediateFuture(bytes[0]);
                } else {
                    return Futures.immediateFuture(null);
                }
            }
        });
    }

    @Override
    public ListenableFuture<byte[][]> dequeueAsync(int n) {
        ListenableFuture<byte[][]> dequeueFuture;
        synchronized (futureLock) {
            if(this.isEmpty()) {
                this.batchSize = n;
                SettableFuture<byte[][]> settableFuture = SettableFuture.create();
                boolean success = this.futureQueue.offer(new FutureRequest(n, settableFuture));
                if(!success) {
                    dequeueFuture = Futures.immediateFailedFuture(new Exception("maximum amount of outstanding requests reached"));
                } else {
                    dequeueFuture = settableFuture;
                }
            } else {
                try {
                    dequeueFuture = Futures.immediateFuture(this.dequeue(n));
                } catch (IOException e) {
                    dequeueFuture = Futures.immediateFailedFuture(e);
                }

            }
        }
        return dequeueFuture;
    }



    @Override
    public void removeAll() throws IOException {
        try {
            queueFrontWriteLock.lock();
            this.innerArray.removeAll();
            this.queueFrontIndex.set(0L);
            IMappedPage queueFrontIndexPage = this.queueFrontIndexPageFactory.acquirePage(QUEUE_FRONT_PAGE_INDEX);
            ByteBuffer queueFrontIndexBuffer = queueFrontIndexPage.getLocal(0);
            queueFrontIndexBuffer.putLong(0L);
            queueFrontIndexPage.setDirty(true);
        } finally {
            queueFrontWriteLock.unlock();
        }
    }

    @Override
    public byte[] peek() throws IOException {
        byte[][] data = this.peek(1);
        if(data != null) {
            return data[0];
        } else {
            return null;
        }
    }

    @Override
    public byte[][] peek(int n) throws IOException {
        if (this.isEmpty()) {
            return null;
        }
        byte[][] buffer = new byte[n][];
        for(int i=0; i<n && this.queueFrontIndex.get()+i < this.innerArray.getHeadIndex(); i++) {
            buffer[i] = this.innerArray.get(this.queueFrontIndex.get()+i);
        }
        return buffer;
    }

    /**
     * apply an implementation of a ItemIterator interface for each queue item
     *
     * @param iterator
     * @throws IOException
     */
    @Override
    public void applyForEach(ItemIterator iterator) throws IOException {
        try {
            queueFrontWriteLock.lock();
            if (this.isEmpty()) {
                return;
            }

            long index = this.queueFrontIndex.get();
            for (long i = index; i < this.innerArray.size(); i++) {
                iterator.forEach(this.innerArray.get(i));
            }
        } finally {
            queueFrontWriteLock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        if (this.queueFrontIndexPageFactory != null) {
            this.queueFrontIndexPageFactory.releaseCachedPages();
        }

        synchronized (futureLock) {
            /* Cancel the future but don't interrupt running tasks
            because they might perform further work not refering to the queue
             */
            for (FutureRequest futureRequest : futureQueue) {
                if(futureRequest != null) {
                    futureRequest.getResultFuture().cancel(false);
                }
            }
        }

        this.innerArray.close();
    }

    @Override
    public void gc() throws IOException {
        long beforeIndex = this.queueFrontIndex.get();
        if (beforeIndex == 0L) { // wrap
            beforeIndex = Long.MAX_VALUE;
        } else {
            beforeIndex--;
        }
        try {
            this.innerArray.removeBeforeIndex(beforeIndex);
        } catch (IndexOutOfBoundsException ex) {
            // ignore
        }
    }

    @Override
    public void flush() {
        try {
            queueFrontWriteLock.lock();
            this.queueFrontIndexPageFactory.flush();
            this.innerArray.flush();
        } finally {
            queueFrontWriteLock.unlock();
        }

    }

    @Override
    public long size() {
        long qFront = this.queueFrontIndex.get();
        long qRear = this.innerArray.getHeadIndex();
        if (qFront <= qRear) {
            return (qRear - qFront);
        } else {
            return Long.MAX_VALUE - qFront + 1 + qRear;
        }
    }

}
