
package dev.test;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import net.spy.memcached.PersistTo;
import net.spy.memcached.ReplicateTo;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;

/**
 * Couchbase load test
 *
 * @version 0.1 Dec 24, 2013
 * @author ilndinesh
 * @since 0.1
 */
public class CouchbaseLoadTest {

    public static void loadProps(final String[] props,
            final Map<String, Object> propMap, final Map<String, String> rawMap) {
        for (final String pair : props) {
            final String[] info = pair.split("=");
            rawMap.put(info[0], info[1]);
            try {
                final Integer value = Integer.parseInt(info[1]);
                propMap.put(info[0], value);
            } catch (final NumberFormatException nfe) {
                propMap.put(info[0], info[1]);
            }
        }
    }

    public static final String[] DEFAULTS = new String[]{
            "cluster=localhost:8091", "bucketName=test", "bucketUser=test",
            "bucketPassword=test", "recordCount=10000", "recordDelay=1000",
            "recordStep=1000", "readCount=1000", "writeCount=500",
            "idPrefix=id-", "readDelay=1000", "writeDelay=1000",
            "loopCount=10", "threadCount=1", "removeRecords=0",
            "printResults=0",};

    private final Map<String, Object> propMap = new HashMap<String, Object>();

    private final Map<String, String> rawMap = new HashMap<String, String>();

    private boolean createDone, readDone, writeDone;

    private final AtomicInteger createTotal = new AtomicInteger();

    private final AtomicInteger readTotal = new AtomicInteger();

    private final AtomicInteger writeTotal = new AtomicInteger();

    public CouchbaseLoadTest(final String[] args) {
        loadProps(DEFAULTS);
        loadProps(args);
    }

    /**
     * @return the propMap
     */
    public Map<String, Object> getPropMap() {
        return propMap;
    }

    /**
     * @return the rawMap
     */
    public Map<String, String> getRawMap() {
        return rawMap;
    }

    /**
     * @return the createTotal
     */
    public AtomicInteger getCreateTotal() {
        return createTotal;
    }

    /**
     * @return the readTotal
     */
    public AtomicInteger getReadTotal() {
        return readTotal;
    }

    /**
     * @return the writeTotal
     */
    public AtomicInteger getWriteTotal() {
        return writeTotal;
    }

    /**
     * @return the createDone
     */
    public boolean isCreateDone() {
        return createDone;
    }

    /**
     * @param createDone
     *            the createDone to set
     */
    public void setCreateDone(final boolean createDone) {
        this.createDone = createDone;
    }

    /**
     * @return the readDone
     */
    public boolean isReadDone() {
        return readDone;
    }

    /**
     * @param readDone
     *            the readDone to set
     */
    public synchronized void setReadDone(final boolean readDone) {
        this.readDone = readDone;
        this.notifyAll();
    }

    /**
     * @return the writeDone
     */
    public boolean isWriteDone() {
        return writeDone;
    }

    /**
     * @param writeDone
     *            the writeDone to set
     */
    public synchronized void setWriteDone(final boolean writeDone) {
        this.writeDone = writeDone;
        this.notifyAll();
    }

    public void loadProps(final String[] props) {
        loadProps(props, propMap, rawMap);
    }

    public void run() throws Exception {
        final String cluster = (String) propMap.get("cluster");
        final String bucketName = (String) propMap.get("bucketName");
        final String bucketUser = (String) propMap.get("bucketUser");
        final String bucketPassword = (String) propMap.get("bucketPassword");
        final int recordCount = (Integer) propMap.get("recordCount");
        // int clientPort = (Integer) propMap.get("clientPort");
        final int recordDelay = (Integer) propMap.get("recordDelay");
        final int recordStep = (Integer) propMap.get("recordStep");
        final int readCount = (Integer) propMap.get("readCount");
        final int writeCount = (Integer) propMap.get("writeCount");
        final int readDelay = (Integer) propMap.get("readDelay");
        final int writeDelay = (Integer) propMap.get("writeDelay");
        final String idPrefix = (String) propMap.get("idPrefix");
        final int loopCount = (Integer) propMap.get("loopCount");
        final int threadCount = (Integer) propMap.get("threadCount");
        final int removeRecords = (Integer) propMap.get("removeRecords");
        final int printResults = (Integer) propMap.get("printResults");
        final int readTotalCount = readCount * loopCount;
        final int writeTotalCount = writeCount * loopCount;
        final PersistTo persistTo = PersistTo.ZERO;
        final ReplicateTo replicateTo = ReplicateTo.ZERO;
        // new AtomicInteger();
        // System.nanoTime();
        final ThreadPoolExecutor storePool = new ThreadPoolExecutor(
                threadCount, threadCount, 0, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new ThreadPoolExecutor.CallerRunsPolicy());
        final List<URI> nodes = new ArrayList<URI>();
        for (final String couchbaseNode : cluster.split(",")) {
            nodes.add(URI.create("http://" + couchbaseNode + "/pools"));
        }
        final CouchbaseConnectionFactoryBuilder factoryBuilder = new CouchbaseConnectionFactoryBuilder();
        factoryBuilder.setOpQueueMaxBlockTime(4000);
        factoryBuilder.setShouldOptimize(true);
        factoryBuilder.setObsPollMax(2);
        factoryBuilder.setObsPollMax(1000);
        factoryBuilder.setOpTimeout(3000);
        factoryBuilder.setViewTimeout(3000);
        final CouchbaseClient client = new CouchbaseClient(
                factoryBuilder.buildCouchbaseConnection(nodes, bucketName,
                        bucketUser, bucketPassword));
        Runtime.getRuntime().addShutdownHook(new Thread() {

            /*
             * (non-Javadoc)
             *
             * @see java.lang.Thread#run()
             */
            @Override
            public void run() {
                if (removeRecords == 1) {
                    // System.out.println("Removing records.");
                    long startTime, endTime;
                    String key;
                    boolean status = false;
                    for (int index = 0; index < recordCount; index++) {
                        key = idPrefix + index;
                        startTime = System.nanoTime();
                        try {
                            status = client.delete(key, persistTo, replicateTo)
                                    .get();
                        } catch (final Exception e) {
                            e.printStackTrace();
                        }
                        endTime = System.nanoTime();
                        if (printResults == 1) {
                            System.out.println("remove," + index + ","
                                    + (status ? (endTime - startTime) : -1));
                        }
                    }
                    client.shutdown();
                }
            }

        });
        // System.out.println("Creating records.");
        if (printResults == 1) {
            System.out.println("Operation" + "," + "Record" + "," + "Time"
                    + "," + "Iteration");
        }
        final long createStart = System.nanoTime();
        for (int index = 0; index < recordCount; index++) {
            if (index % recordStep == 0) {
                Thread.sleep(recordDelay);
            }
            final int currentIndex = index;
            storePool.execute(new Runnable() {

                public void run() {
                    long startTime, endTime;
                    final String key = idPrefix + currentIndex;
                    final String bean = "{\"id\":\"" + key + "\"}";
                    boolean status = false;
                    startTime = System.nanoTime();
                    try {
                        status = client.set(key, bean, persistTo, replicateTo)
                                .get();
                    } catch (final Exception e) {
                        e.printStackTrace();
                    }
                    endTime = System.nanoTime();
                    if (printResults == 1) {
                        System.out.println("create," + currentIndex + ","
                                + (status ? (endTime - startTime) : -1));
                    }
                    if (createTotal.incrementAndGet() == recordCount) {
                        synchronized (CouchbaseLoadTest.this) {
                            createDone = true;
                            CouchbaseLoadTest.this.notifyAll();
                        }
                    }
                }
            });
        }
        if (recordCount > 0) {
            synchronized (this) {
                while (!createDone) {
                    this.wait();
                }
            }
        }
        final long createStop = System.nanoTime();
        if (printResults == 1) {
            System.out.println("create-done,," + (createStop - createStart));
        }
        final long readWriteStart = System.nanoTime();
        new Thread() {

            /*
             * (non-Javadoc)
             *
             * @see java.lang.Thread#run()
             */
            @Override
            public void run() {
                for (int index = 1; index <= loopCount; index++) {
                    try {
                        Thread.sleep(readDelay);
                    } catch (final InterruptedException e) {
                        e.printStackTrace();
                    }
                    final int currentIndex = index;
                    for (int opIndex = 0; opIndex < readCount; opIndex++) {
                        final int currentOpIndex = opIndex;
                        storePool.execute(new Runnable() {

                            public void run() {
                                long startTime, endTime;
                                String key;
                                key = idPrefix + currentOpIndex;
                                startTime = System.nanoTime();
                                client.get(key);
                                endTime = System.nanoTime();
                                if (printResults == 1) {
                                    System.out.println("read," + currentOpIndex
                                            + "," + (endTime - startTime) + ","
                                            + currentIndex);
                                }
                                if (readTotal.incrementAndGet() == readTotalCount) {
                                    synchronized (CouchbaseLoadTest.this) {
                                        readDone = true;
                                        CouchbaseLoadTest.this.notifyAll();
                                    }
                                }
                            }
                        });
                    }
                }
                // setReadDone(true);
            }

        }.start();
        new Thread() {

            /*
             * (non-Javadoc)
             *
             * @see java.lang.Thread#run()
             */
            @Override
            public void run() {
                for (int index = 1; index <= loopCount; index++) {
                    try {
                        Thread.sleep(writeDelay);
                    } catch (final InterruptedException e) {
                        e.printStackTrace();
                    }
                    final int currentIndex = index;
                    for (int opIndex = 0; opIndex < writeCount; opIndex++) {
                        final int currentOpIndex = opIndex;
                        storePool.execute(new Runnable() {

                            public void run() {
                                long startTime, endTime;
                                String bean;
                                boolean status = false;
                                final String key = idPrefix + currentIndex;
                                bean = "{\"id\":\"" + key + "\"}";
                                startTime = System.nanoTime();
                                try {
                                    status = client.set(key, bean, persistTo,
                                            replicateTo).get();
                                } catch (final Exception e) {
                                    e.printStackTrace();
                                }
                                endTime = System.nanoTime();
                                if (printResults == 1) {
                                    System.out.println("write,"
                                            + currentOpIndex
                                            + ","
                                            + (status
                                                    ? (endTime - startTime)
                                                    : -1) + "," + currentIndex);
                                }
                                if (writeTotal.incrementAndGet() == writeTotalCount) {
                                    synchronized (CouchbaseLoadTest.this) {
                                        writeDone = true;
                                        CouchbaseLoadTest.this.notifyAll();
                                    }
                                }
                            }
                        });
                    }
                }
                // setWriteDone(true);
            }

        }.start();
        synchronized (this) {
            while ((readCount > 0 && !readDone)
                    || (writeCount > 0 && !writeDone)) {
                this.wait();
            }
        }
        final long readWriteStop = System.nanoTime();
        if (printResults == 1) {
            System.out.println("read-write-done,,"
                    + (readWriteStop - readWriteStart - (loopCount - 1)
                            * (readDelay + writeDelay) * 1000000));
        }
        System.exit(0);
    }

    public static void main(final String args[]) throws Exception {
        new CouchbaseLoadTest(args).run();
    }
}
