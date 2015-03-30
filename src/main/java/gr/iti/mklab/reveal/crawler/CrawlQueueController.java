package gr.iti.mklab.reveal.crawler;

import gr.iti.mklab.reveal.configuration.Configuration;
import gr.iti.mklab.reveal.visual.VisualIndexer;
import gr.iti.mklab.reveal.web.Responses;
import gr.iti.mklab.simmo.items.Image;
import gr.iti.mklab.simmo.items.Video;
import gr.iti.mklab.simmo.jobs.CrawlJob;
import gr.iti.mklab.simmo.jobs.Job;
import gr.iti.mklab.simmo.morphia.MediaDAO;
import gr.iti.mklab.simmo.morphia.MorphiaManager;
import org.apache.commons.lang.ArrayUtils;
import org.bson.types.ObjectId;
import org.mongodb.morphia.dao.BasicDAO;
import org.mongodb.morphia.dao.DAO;
import org.mongodb.morphia.query.Query;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.File;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by kandreadou on 12/18/14.
 */
public class CrawlQueueController {
    private DAO<CrawlJob, ObjectId> dao;
    private Poller poller;

    public CrawlQueueController() {
        // Creates a DAO object to persist submitted crawl requests
        dao = new BasicDAO<>(CrawlJob.class, MorphiaManager.getMongoClient(), MorphiaManager.getMorphia(), MorphiaManager.getCrawlsDB().getName());
        // Starts a polling thread to regularly check for empty slots
        poller = new Poller();
        poller.startPolling();
    }

    public static void main(String[] args) throws Exception {
        Configuration.load("local.properties");
        MorphiaManager.setup("127.0.0.1");
        VisualIndexer.init();
        CrawlQueueController controller = new CrawlQueueController();
        String colName = "tsipras";
        Set<String> keywords = new HashSet<String>();
        keywords.add("tsipras");
        controller.submit(true, colName, keywords);
    }

    public void shutdown() {

        poller.stopPolling();
        List<CrawlJob> list = getRunningCrawls();
        for (CrawlJob job:list){
            cancel(job.getId());
        }
    }

    /**
     * Submits a new crawl request
     *
     * @param collectionName
     */
    public synchronized CrawlJob submit(boolean isNew, String collectionName, Set<String> keywords) throws Exception {
        System.out.println("CRAWL: submit "+collectionName+" keywords " + ArrayUtils.toString(keywords));
        String crawlDataPath = Configuration.CRAWLS_DIR + collectionName;
        List<CrawlJob> requestsWithSameName = dao.getDatastore().find(CrawlJob.class).field("collection").equal(collectionName).asList();
        if (!isNew && (new File(crawlDataPath).exists() || requestsWithSameName.size() > 0))
            throw new Exception("The collection " + collectionName + " already exists. Choose a different name or mark not new");
        CrawlJob r = new CrawlJob(crawlDataPath, collectionName, keywords, isNew);
        r.setState(Job.STATE.WAITING);
        r.setLastStateChange(new Date());
        r.setCreationDate(new Date());
        dao.save(r);
        tryLaunch();
        return r;
    }

    public synchronized CrawlJob cancel(String id) {
        System.out.println("CRAWL: Cancel for id " + id);
        CrawlJob req = getCrawlRequest(id);
        System.out.println("CrawlRequest " + req.getCollection() + " " + req.getState());
        req.setState(CrawlJob.STATE.STOPPING);
        req.setLastStateChange(new Date());
        dao.save(req);
        cancelForName(req.getCollection());
        return req;
    }

    public synchronized CrawlJob delete(String id) throws Exception {
        System.out.println("CRAWL: Delete for id " + id);
        CrawlJob req = getCrawlRequest(id);
        System.out.println("CrawlRequest " + req.getCollection() + " " + req.getState());
        req.setState(CrawlJob.STATE.DELETING);
        req.setLastStateChange(new Date());
        dao.save(req);
        cancelForName(req.getCollection());
        return req;
    }

    private void tryLaunch() throws Exception {
        List<CrawlJob> list = getRunningCrawls();
        System.out.println("Running crawls list size " + list.size());

        if (list.size() >= 2)
            return;
        List<CrawlJob> waitingList = getWaitingCrawls();
        if (waitingList.isEmpty())
            return;
        CrawlJob req = waitingList.get(0);
        req.setState(CrawlJob.STATE.STARTING);
        dao.save(req);
        startCrawl(req);
    }

    private void startCrawl(CrawlJob req) throws Exception {
        System.out.println("METHOD: Startcrawl " + req.getCollection() + " " + req.getState());
        new Thread(new RevealAgent("127.0.0.1", 9999, req)).start();
    }

    /**
     * Cancels the BUbiNG Agent listening to the specified port
     */
    private void cancelForName(String name) {
        try {
//JMXServiceURL jmxServiceURL = new JMXServiceURL("service:jmx:rmi://localhost/jndi/rmi://localhost:9999/jmxrmi");
            JMXServiceURL jmxServiceURL = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://127.0.0.1:9999/jmxrmi");
            JMXConnector cc = JMXConnectorFactory.connect(jmxServiceURL);
            MBeanServerConnection mbsc = cc.getMBeanServerConnection();
//This information is available in jconsole
            ObjectName serviceConfigName = new ObjectName("it.unimi.di.law.bubing:type=Agent,name=" + name);
// Invoke stop operation
            mbsc.invoke(serviceConfigName, "stop", null, null);
// Close JMX connector
            cc.close();
        } catch (Exception e) {
            System.out.println("Exception occurred: " + e.toString());
            e.printStackTrace();
        }
    }

    //////////////////////////////////////////////////
    //////////// POLLING ///////////////////////////
    /////////////////////////////////////////////////

    public class Poller implements Runnable {
        final ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
        Future future = null;

        @Override
        public void run() {
            System.out.println("NEW polling event");
            try {
                tryLaunch();
            } catch (Exception ex) {
                System.out.println(ex);
            }
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

    //////////////////////////////////////////////////
    //////////// DB STUFF ///////////////////////////
    /////////////////////////////////////////////////

    private CrawlJob getCrawlRequest(String id) {
        return dao.findOne("_id", id);
    }

    private List<CrawlJob> getRunningCrawls() {
        Query<CrawlJob> q = dao.createQuery();
        q.or(
                q.criteria("requestState").equal(CrawlJob.STATE.RUNNING),
                q.criteria("requestState").equal(CrawlJob.STATE.STOPPING),
                q.criteria("requestState").equal(CrawlJob.STATE.DELETING),
                q.criteria("requestState").equal(CrawlJob.STATE.STARTING)
        );
        return q.asList();
    }

    private List<CrawlJob> getWaitingCrawls() {
        return dao.getDatastore().find(CrawlJob.class).filter("requestState", CrawlJob.STATE.WAITING).asList();
    }

    /**
     * WAITING and RUNNING crawls
     *
     * @return
     */
    public List<Responses.CrawlStatus> getActiveCrawls() {
        Query<CrawlJob> q = dao.createQuery();
        q.or(
                q.criteria("requestState").equal(CrawlJob.STATE.RUNNING),
                q.criteria("requestState").equal(CrawlJob.STATE.WAITING),
                q.criteria("requestState").equal(CrawlJob.STATE.STOPPING),
                q.criteria("requestState").equal(CrawlJob.STATE.FINISHED),
                q.criteria("requestState").equal(CrawlJob.STATE.DELETING),
                q.criteria("requestState").equal(CrawlJob.STATE.STARTING)
        );
        List<Responses.CrawlStatus> result = new ArrayList<>();
        for (CrawlJob req : q.asList()) {
            result.add(getStatusFromCrawlRequest(req));
        }
        return result;
    }

    public synchronized Responses.CrawlStatus getStatus(String id) {
        return getStatusFromCrawlRequest(getCrawlRequest(id));
    }

    private Responses.CrawlStatus getStatusFromCrawlRequest(CrawlJob req) {
        Responses.CrawlStatus status = new Responses.CrawlStatus(req);
        MediaDAO<Image> imageDAO = new MediaDAO<>(Image.class, status.getCollection());
        MediaDAO<Video> videoDAO = new MediaDAO<>(Video.class, status.getCollection());
        Date lastImageInserted = null;
        Date lastVideoInserted = null;
        if (imageDAO != null && imageDAO.count() > 0) {
            status.numImages = imageDAO.count();
            status.image = getRepresentativeImage(imageDAO, status.getKeywords());
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
        switch (status.getState()) {
            case WAITING:
                status.duration = 0;
                break;
            case RUNNING:
            case STOPPING:
                status.duration = new Date().getTime() - status.getCreationDate().getTime();
                break;
            default:
                status.duration = status.getLastStateChange().getTime() - status.getCreationDate().getTime();
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
}
