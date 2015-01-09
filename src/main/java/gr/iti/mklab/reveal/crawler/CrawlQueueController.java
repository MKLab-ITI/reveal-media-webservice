package gr.iti.mklab.reveal.crawler;

import gr.iti.mklab.simmo.items.Image;
import gr.iti.mklab.simmo.morphia.MorphiaManager;
import org.apache.commons.lang.ArrayUtils;
import org.bson.types.ObjectId;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.dao.BasicDAO;
import org.mongodb.morphia.dao.DAO;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by kandreadou on 12/18/14.
 */
public class CrawlQueueController {
    public static final String DB_NAME = "crawlerQUEUE";
    private DAO<CrawlRequest, ObjectId> dao;
    private Poller poller;

    /**
     * The number of AVAILABLE_PORTS defines the number of simultaneously running BUbiNG Agents
     */
    private final static Integer[] AVAILABLE_PORTS = {9995, 9997, 9999};

    public CrawlQueueController() {
        // Sets up the Morphia Manager
        MorphiaManager.setup(DB_NAME);
        // Creates a DAO object to persist submitted crawl requests
        dao = new BasicDAO<>(CrawlRequest.class, MorphiaManager.getMongoClient(), MorphiaManager.getMorphia(), MorphiaManager.getDB().getName());
        // Starts a polling thread to regularly check for empty slots
        poller = new Poller();
        poller.startPolling();
    }

    public void shutdown() {
        MorphiaManager.tearDown();
        poller.stopPolling();
    }

    /**
     * Submits a new crawl request
     *
     * @param crawlDir
     * @param collectionName
     */
    public synchronized CrawlRequest submit(boolean isNew, String crawlDir, String collectionName, String... keywords) {
        System.out.println("submit event "+ ArrayUtils.toString(keywords));
        CrawlRequest r = enqueue(isNew, crawlDir, collectionName, keywords);
        tryLaunch();
        return r;
    }

    public synchronized CrawlRequest cancel(String id) {
        CrawlRequest req = getCrawlRequest(id).get(0);
        req.requestState = CrawlRequest.STATE.STOPPING;
        dao.save(req);
        cancelForPort(req.portNumber);
        return req;
    }

    public synchronized  CrawlRequest getStatus(String id){
        CrawlRequest req = getCrawlRequest(id).get(0);
        DAO<Image, ObjectId> images = new BasicDAO<>(Image.class, MorphiaManager.getMongoClient(), MorphiaManager.getMorphia(), req.collectionName);
        req.numImages = (int) images.count();
        dao.save(req);
        return req;
    }

    public List<Image> getImages(String id, int count, int offset){
        CrawlRequest req = getCrawlRequest(id).get(0);
        Datastore ds = MorphiaManager.getMorphia().createDatastore(MorphiaManager.getMongoClient(),req.collectionName);
        return ds.find(Image.class).offset(offset).limit(count).asList();
    }

    /**
     * Cancels the BUbiNG Agent listening to the specified port
     *
     * @param portNumber
     */
    private void cancelForPort(int portNumber) {
        try {
            //JMXServiceURL jmxServiceURL = new JMXServiceURL("service:jmx:rmi://localhost/jndi/rmi://localhost:9999/jmxrmi");
            JMXServiceURL jmxServiceURL = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://127.0.0.1:" + portNumber + "/jmxrmi");
            JMXConnector cc = JMXConnectorFactory.connect(jmxServiceURL);
            MBeanServerConnection mbsc = cc.getMBeanServerConnection();
            //This information is available in jconsole
            ObjectName serviceConfigName = new ObjectName("it.unimi.di.law.bubing:type=Agent,name=agent");
            //  Invoke stop operation
            mbsc.invoke(serviceConfigName, "stop", null, null);
            //  Close JMX connector
            cc.close();
        } catch (Exception e) {
            System.out.println("Exception occurred: " + e.toString());
            e.printStackTrace();
        }
    }

    private CrawlRequest enqueue(boolean isNew, String crawlDir, String collectionName, String... keywords) {
        CrawlRequest r = new CrawlRequest();
        r.collectionName = collectionName;
        r.requestState = CrawlRequest.STATE.WAITING;
        r.lastStateChange = new Date();
        r.creationDate = new Date();
        r.crawlDataPath = crawlDir;
        r.isNew = isNew;
        for (String k : keywords)
            r.keywords.add(k);
        dao.save(r);
        return r;
    }

    private void tryLaunch() {
        List<CrawlRequest> list = getRunningCrawls();
        // Make a copy of the available port numbers
        List<Integer> ports = new LinkedList<Integer>(Arrays.asList(AVAILABLE_PORTS));
        // and find a non-used port
        System.out.println("Running crawls list size " + list.size());
        for (CrawlRequest r : list) {
            System.out.println("Port " + r.portNumber);
            if (ports.contains(r.portNumber)) {
                // Check if the Agent on that port has finished or failed
                // without updating the DB
                if (isPortAvailable(r.portNumber)) {
                    System.out.println("Available");
                    r.requestState = CrawlRequest.STATE.FINISHED;
                    r.lastStateChange = new Date();
                    dao.save(r);
                }
                // The port is really busy so remove it from the list of available ports
                else {
                    System.out.println("Not available");
                    ports.remove(new Integer(r.portNumber));
                }
            }
        }
        List<CrawlRequest> waitingList = getWaitingCrawls();
        if(waitingList.isEmpty())
            return;
        for (Integer i : ports) {
            System.out.println("Try launch crawl for port " + i);
            // Check if port is really available, if it is launch the respective script
            if (isPortAvailable(i)) {
                launch("crawl" + i + ".sh");
                break;
            }
            System.out.println("Port " + i + " is not available");
        }
    }

    private void launch(String scriptName) {

        try {
            String path = "/home/iti-310/vdata/" + scriptName;
            String[] command = {path};
            ProcessBuilder p = new ProcessBuilder(command);
            Process pr = p.start();
            inheritIO(pr.getInputStream(), System.out);
            inheritIO(pr.getErrorStream(), System.err);
        } catch (IOException ioe) {
            System.out.println("Problem starting process for scriptName " + scriptName + " " + ioe);
        }
    }

    public class Poller implements Runnable {
        final ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
        Future future = null;

        @Override
        public void run() {
            System.out.println("NEW polling event");
            tryLaunch();
        }

        public void startPolling() {
            exec.scheduleAtFixedRate(this, 10, 60, TimeUnit.SECONDS);
        }

        public void stopPolling() {
            if (future != null && !future.isDone())
                future.cancel(true);
            exec.shutdownNow();
        }
    }

    private List<CrawlRequest> getCrawlRequest(String id) {
        return dao.getDatastore().find(CrawlRequest.class).filter("id", id).asList();
    }

    private List<CrawlRequest> getRunningCrawls() {
        return dao.getDatastore().find(CrawlRequest.class).filter("requestState", CrawlRequest.STATE.RUNNING).asList();
    }

    private List<CrawlRequest> getWaitingCrawls() {
        return dao.getDatastore().find(CrawlRequest.class).filter("requestState", CrawlRequest.STATE.WAITING).asList();
    }

    private boolean isPortAvailable(int port) {

        ServerSocket ss = null;
        DatagramSocket ds = null;
        try {
            ss = new ServerSocket(port);
            ss.setReuseAddress(true);
            ds = new DatagramSocket(port);
            ds.setReuseAddress(true);
            return true;
        } catch (IOException e) {
        } finally {
            if (ds != null) {
                ds.close();
            }

            if (ss != null) {
                try {
                    ss.close();
                } catch (IOException e) {
                /* should not be thrown */
                }
            }
        }

        return false;
    }

    private static void inheritIO(final InputStream src, final PrintStream dest) {
        new Thread(new Runnable() {
            public void run() {
                Scanner sc = new Scanner(src);
                while (sc.hasNextLine()) {
                    dest.println(sc.nextLine());
                }
            }
        }).start();
    }
}
