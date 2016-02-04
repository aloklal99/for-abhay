package alok.tagsync;

import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by alal on 2/3/16.
 */
public class OneSynchrenousQueue {
    private static final Logger LOG = Logger.getLogger(OneSynchrenousQueue.class);
    static final Random _Random = new Random(1234); // shared random number generator

    public static void main(String[] args) {

        int numProducers = 1 + _Random.nextInt(5); // create 1 through 5 producers
        int numConsumers = 1;
        int total = numConsumers + numProducers;
        Base._Latch = new CountDownLatch(total);

        final BlockingQueue<TagUpdate> queue = new SynchronousQueue<TagUpdate>();
        ExecutorService executor = Executors.newFixedThreadPool(total, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setDaemon(false);
                return t;
            }
        });

        Sender sender = new Sender(queue);
        LOG.info("Starting consumer.");
        executor.submit(sender);

        LOG.info("Starting " + numProducers + " producers.");
        for (int i = 0; i < numProducers; i++) {
            executor.submit(new Producer(queue));
        }
        try {
            Base._Latch.await();
            LOG.info("All consumers and producers have been scheduled!");
            int sleepFor = 5 + _Random.nextInt(6); // sleep for 5 - 10 seconds
            LOG.info("Main thread sleeping for " + sleepFor + " s.");
            Thread.sleep(sleepFor * 1000);
            executor.shutdownNow();
        } catch (InterruptedException e) {
            LOG.error("Unexpected: Main thread interupted while sleeping!!");
        }
    }

    static class Producer extends Base {
        protected static final String _Type = "P";

        Producer(BlockingQueue<TagUpdate> queue) {
            super(Producer._Type, queue);
        }

        private TagUpdate peekNotification() {
            return new TagUpdate();
        }

        private void popNotification() {
            // do nothing
        }

        @Override
        void process() throws InterruptedException {
            TagUpdate newEvent = peekNotification();

            exchange(newEvent);

            log("Popping", newEvent);
            popNotification();
        }

        void exchange(TagUpdate event) throws InterruptedException {
            // can't let multiple producers enter here else they would start exchanging with each other!
            synchronized (Producer.class) {
                log("Trying to put", event);
                _queue.put(event);

                log("Done putting", event);
                TagUpdate receivedEvent = _queue.take();
                log("Received back", event);

                if (event != receivedEvent) {
                    fatalError("Received event not same as sent event!");
                }
            }
        }
    }

    static class Sender extends Base {
        protected static final String _Type = "S";

        Sender(BlockingQueue<TagUpdate> queue) {
            super(Sender._Type, queue);
        }

        private void send(TagUpdate toSend) throws InterruptedException {
            int sleepFor = _Random.nextInt(2000); // sleep for upto 2 seconds
            Thread.sleep(sleepFor);
        }

        @Override
        void process() throws InterruptedException {
            log("Awaiting receipt", (TagUpdate)null);
            TagUpdate newEvent = _queue.take();
            log("Sending", newEvent);
            send(newEvent);
            log("Signalling", newEvent);
            _queue.put(newEvent);
        }
    }

    static abstract class Base implements Runnable {
        // only to identify log messages given by multiple producers
        static final Map<String, AtomicInteger> _Counters = new HashMap<String, AtomicInteger>();
        static {
            _Counters.put(Producer._Type, new AtomicInteger(0));
            _Counters.put(Sender._Type, new AtomicInteger(0));
        }
        // to ensure everyone starts at the sametime
        static CountDownLatch _Latch;
        // state
        final int _id;
        final String _type;
        final BlockingQueue<TagUpdate> _queue;

        Base(String type, BlockingQueue<TagUpdate> queue) {
            _type = type;
            _id = _Counters.get(type).incrementAndGet();
            _queue = queue;
        }

        void log(String message, Throwable t) {
            LOG.warn(String.format("%s #%d: %s", _type, _id, message), t);
        }

        void log(String message) {
            LOG.info(String.format("%s #%d: %s", _type, _id, message));
        }

        void log(String action, TagUpdate tagUpdate) {
//            String message = String.format("%s #%d: %10s: %s", _type, _id, action, tagUpdate);
            String message = String.format("%s #%d: %15s", _type, _id, action) + (tagUpdate == null ? "" : ": " + tagUpdate);
            LOG.info(message);
        }

        void fatalError(String error) {
            fatalError(error, null);
        }

        void fatalError(String error, Throwable t) {
            String message = String.format("%s #%d: %s", _type, _id, error);
            LOG.error(message);
            if (t != null) {
                throw new RuntimeException(message, t);
            }
        }

        @Override
        public void run() {
            try {
                log("Starting...");
                _Latch.countDown();
                try {
                    _Latch.await();
                } catch (InterruptedException e) {
                    fatalError("Interrupted while awaiting on latch!");
                }
                while (true) {
                    try {
                        process();
                    } catch (InterruptedException e) {
                        log("Interrupted!  Exiting...", e);
                        break;
                    }
                }
            } catch (Throwable t) {
                log("Unexpected error", t);
            } finally {
                log("Exiting...");
            }
        }

        abstract void process() throws InterruptedException;
    }

    static class TagUpdate {
        final int _value;

        TagUpdate() {
            _value = _Random.nextInt();
        }

        @Override
        public String toString() {
            return "" + _value;
        }
    }
}
