package gr.iti.mklab.reveal.crawler;

import gr.iti.mklab.reveal.configuration.Configuration;
import gr.iti.mklab.reveal.web.Responses;
import gr.iti.mklab.simmo.items.Image;
import gr.iti.mklab.simmo.items.Video;
import gr.iti.mklab.simmo.morphia.MediaDAO;
import gr.iti.mklab.simmo.morphia.MorphiaManager;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.ArrayUtils;
import org.bson.types.ObjectId;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.dao.BasicDAO;
import org.mongodb.morphia.dao.DAO;
import org.mongodb.morphia.query.Query;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.File;
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
    private Map<String, RevealAgent> agents = new HashMap<>(3);

    public CrawlQueueController() {
        // Creates a DAO object to persist submitted crawl requests
        dao = new BasicDAO<>(CrawlRequest.class, MorphiaManager.getMongoClient(), MorphiaManager.getMorphia(), MorphiaManager.getDB(DB_NAME).getName());
        Set<String> k = new HashSet<String>();
        k.add("the");
        enqueue(true, "test",k);
        // Starts a polling thread to regularly check for empty slots
        poller = new Poller();
        poller.startPolling();
    }

    public static void main(String[] args) {
        MorphiaManager.setup("127.0.0.1");
        CrawlQueueController controller = new CrawlQueueController();
    }

    public void shutdown() {
        poller.stopPolling();
    }

    /**
     * Submits a new crawl request
     *
     * @param crawlDir
     * @param collectionName
     */
    public synchronized CrawlRequest submit(boolean isNew, String crawlDir, String collectionName, Set<String> keywords) throws Exception {
        System.out.println("submit event " + ArrayUtils.toString(keywords));
        if (!isNew && new File(crawlDir).exists())
            throw new Exception("The collection " + collectionName + " already exists. Choose a different name");
        CrawlRequest r = enqueue(isNew, collectionName.toLowerCase(), keywords);
        tryLaunch();
        return r;
    }

    public synchronized CrawlRequest cancel(String id) {
        System.out.println("CRAWL: Cancel for id " + id);
        CrawlRequest req = getCrawlRequest(id);
        if ("showcase".equalsIgnoreCase(req.collectionName))
            return null;
        System.out.println("CrawlRequest " + req.collectionName + " " + req.requestState);
        req.requestState = CrawlRequest.STATE.STOPPING;
        dao.save(req);
        agents.get(req.collectionName).stop();
        return req;
    }

    public synchronized void delete(String id) throws Exception {
        System.out.println("CRAWL: Delete for id " + id);
        CrawlRequest req = getCrawlRequest(id);
        if ("showcase".equalsIgnoreCase(req.collectionName))
            return;
        System.out.println("CrawlRequest " + req.collectionName + " " + req.requestState);
        if (req != null) {
            if (req.requestState != CrawlRequest.STATE.RUNNING) {
                //Delete the request from the request DB
                dao.delete(req);
                //Delete the collection DB
                MorphiaManager.getDB(req.collectionName).dropDatabase();
                //Delete the crawl and index folders
                FileUtils.deleteDirectory(new File(req.crawlDataPath));
                FileUtils.deleteDirectory(new File(Configuration.INDEX_FOLDER + req.collectionName));
                // Remove the agent from the map
                agents.remove(req.collectionName);
            } else {
                agents.get(req.collectionName).stop();
                req.requestState = CrawlRequest.STATE.DELETING;
                dao.save(req);
            }
        }
    }

    public List<Image> getImages(String id, int count, int offset) {
        CrawlRequest req = getCrawlRequest(id);
        Datastore ds = MorphiaManager.getMorphia().createDatastore(MorphiaManager.getMongoClient(), req.collectionName);
        return ds.find(Image.class).offset(offset).limit(count).asList();
    }

    private CrawlRequest enqueue(boolean isNew, String collectionName, Set<String> keywords) {
        CrawlRequest r = new CrawlRequest();
        r.collectionName = collectionName;
        r.requestState = CrawlRequest.STATE.WAITING;
        r.lastStateChange = new Date();
        r.creationDate = new Date();
        r.crawlDataPath = Configuration.CRAWLS_DIR+collectionName;
        r.isNew = isNew;
        r.keywords = keywords;
        dao.save(r);
        return r;
    }

    private void tryLaunch() {
        List<CrawlRequest> list = getRunningCrawls();
        System.out.println("Running crawls list size " + list.size());
        for (CrawlRequest r : list) {
            System.out.println("Port " + r.portNumber);
            if (agents.keySet().contains(r.collectionName)) {
                if (r.requestState == CrawlRequest.STATE.STOPPING || r.requestState == CrawlRequest.STATE.DELETING) {
                    System.out.println("Crawl " + r.id + "  with name " + r.collectionName + "and state " + r.requestState + " has not stopped yet. Trying again");
                    agents.get(r.collectionName).stop();
                }
            }
        }

        List<CrawlRequest> waitingList = getWaitingCrawls();
        if (waitingList.isEmpty())
            return;
        RevealAgent a = new RevealAgent("127.0.0.1", 9999, waitingList.get(0));
        new Thread(a).start();
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

    private CrawlRequest getCrawlRequest(String id) {
        return dao.findOne("_id", id);
    }

    private List<CrawlRequest> getRunningCrawls() {
        Query<CrawlRequest> q = dao.createQuery();
        q.or(
                q.criteria("requestState").equal(CrawlRequest.STATE.RUNNING),
                q.criteria("requestState").equal(CrawlRequest.STATE.STOPPING),
                q.criteria("requestState").equal(CrawlRequest.STATE.DELETING)
        );
        return q.asList();
    }

    private List<CrawlRequest> getWaitingCrawls() {
        return dao.getDatastore().find(CrawlRequest.class).filter("requestState", CrawlRequest.STATE.WAITING).asList();
    }

    /**
     * WAITING and RUNNING crawls
     *
     * @return
     */
    public List<Responses.CrawlStatus> getActiveCrawls() {
        Query<CrawlRequest> q = dao.createQuery();
        q.or(
                q.criteria("requestState").equal(CrawlRequest.STATE.RUNNING),
                q.criteria("requestState").equal(CrawlRequest.STATE.WAITING),
                q.criteria("requestState").equal(CrawlRequest.STATE.STOPPING),
                q.criteria("requestState").equal(CrawlRequest.STATE.FINISHED),
                q.criteria("requestState").equal(CrawlRequest.STATE.DELETING)
        );
        List<Responses.CrawlStatus> result = new ArrayList<>();
        for (CrawlRequest req : q.asList()) {
            result.add(getStatusFromCrawlRequest(req));
        }
        return result;
    }

    public synchronized Responses.CrawlStatus getStatus(String id) {
        return getStatusFromCrawlRequest(getCrawlRequest(id));
    }

    private Responses.CrawlStatus getStatusFromCrawlRequest(CrawlRequest req) {
        Responses.CrawlStatus status = new Responses.CrawlStatus(req);
        MediaDAO<Image> imageDAO = new MediaDAO<>(Image.class, status.collectionName);
        MediaDAO<Video> videoDAO = new MediaDAO<>(Video.class, status.collectionName);
        Date lastImageInserted = null;
        Date lastVideoInserted = null;
        if (imageDAO != null && imageDAO.count() > 0) {
            status.numImages = imageDAO.count();
            status.image = getRepresentativeImage(imageDAO, status.keywords);
            List<Image> imgs = imageDAO.getDatastore().find(Image.class).order("-crawlDate").limit(100).asList();
            if (imgs != null && imgs.size() > 0)
                lastImageInserted = imgs.get(0).getCrawlDate();
        }
        if (videoDAO != null && videoDAO.count() > 0) {
            status.numVideos = videoDAO.count();
            status.video = videoDAO.getItems(1, 0).get(0);
            List<Video> vds = videoDAO.getDatastore().find(Video.class).order("-crawlDate").limit(100).asList();
            if (vds != null && vds.size() > 0)
                lastVideoInserted = vds.get(0).getCrawlDate();
        }
        switch (status.requestState) {
            case WAITING:
                status.duration = 0;
                break;
            case RUNNING:
            case STOPPING:
                status.duration = new Date().getTime() - status.creationDate.getTime();
                break;
            default:
                status.duration = status.lastStateChange.getTime() - status.creationDate.getTime();
                break;
        }
        if (lastImageInserted == null && lastVideoInserted == null) {
            status.lastItemInserted = "-";
        } else if (lastImageInserted != null && lastVideoInserted != null) {
            status.lastItemInserted = (lastImageInserted.after(lastVideoInserted) ? lastImageInserted : lastVideoInserted).toString();
        } else if (lastImageInserted != null) {
            status.lastItemInserted = lastImageInserted.toString();
        } else if (lastVideoInserted != null) {
            status.lastItemInserted = lastVideoInserted.toString();
        }
        return status;
    }

    private Image getRepresentativeImage(MediaDAO<Image> images, Set<String> keywords) {

        int offset = 0;
        int count = (int) images.count();
        if (images.count() > 2501) {
            offset = 500;
            count = 2000;
        }

        List<Image> res = images.search("lastModifiedDate", new Date(0), 500, 400, count, offset);
        if (res == null || res.size() == 0)
            return null;
        for (Image i : res) {

            for (String keyword : keywords) {
                if ((i.getTitle() != null && i.getTitle().contains(keyword)) || (i.getDescription() != null && i.getDescription().contains(keyword)))
                    return i;
            }
        }
        return res.get(0);
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
}
