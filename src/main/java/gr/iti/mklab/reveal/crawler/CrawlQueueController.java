package gr.iti.mklab.reveal.crawler;

import gr.iti.mklab.reveal.util.Configuration;
import gr.iti.mklab.reveal.util.StreamManagerClient;
import gr.iti.mklab.reveal.visual.VisualIndexClient;
import gr.iti.mklab.reveal.web.Responses;
import gr.iti.mklab.simmo.core.items.Image;
import gr.iti.mklab.simmo.core.items.Video;
import gr.iti.mklab.simmo.core.jobs.CrawlJob;
import gr.iti.mklab.simmo.core.morphia.MediaDAO;
import gr.iti.mklab.simmo.core.morphia.MorphiaManager;
import gr.iti.mklab.sm.streams.StreamException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.mongodb.morphia.dao.BasicDAO;
import org.mongodb.morphia.dao.DAO;
import org.mongodb.morphia.query.Query;

import java.io.File;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A controller to monitor crawls. The maximum number of simultaneous crawls
 * is defined in the properties files, in the numCrawls property
 */
public class CrawlQueueController {

	private String indexServiceHost = "http://" + Configuration.INDEX_SERVICE_HOST + ":8080/VisualIndexService";
	
    private StreamManagerClient streamManager;
    private Map<String, GeoCrawler> geoCrawlerMap = new HashMap<>();
    private DAO<CrawlJob, ObjectId> dao;
    private Poller poller;

    private Map<String, RevealAgent> agents = new HashMap<String, RevealAgent>();
    private ExecutorService executorService = Executors.newFixedThreadPool(Configuration.NUM_CRAWLS + 1);
    
    private Logger _logger = Logger.getLogger(CrawlQueueController.class);
    
    public CrawlQueueController() {
        // Creates a DAO object to persist submitted crawl requests
        dao = new BasicDAO<>(CrawlJob.class, MorphiaManager.getMongoClient(), MorphiaManager.getMorphia(), MorphiaManager.getCrawlsDB().getName());
        
        // the client for handling the stream manager social media crawler
        streamManager = new StreamManagerClient("http://" + Configuration.STREAM_MANAGER_SERVICE_HOST + ":8080");
        
        startRunningCrawlsAtStartup();
        deleteAndStopCrawlsAtStartup();
        
        // Starts a polling thread to regularly check for empty slots
        poller = new Poller();
        poller.startPolling();
        
    }


    public void shutdown() throws Exception {
        poller.stopPolling();
        List<CrawlJob> list = getRunningCrawls();
        for (CrawlJob job : list) {
            kill(job.getId());
        }
    }

    /**
     * Submits a new crawl request
     *
     * @param collectionName
     */
    public synchronized CrawlJob submit(boolean isNew, String collectionName, Set<String> keywords) throws Exception {
    	
    	_logger.info("CRAWL: submit " + collectionName + " keywords " + ArrayUtils.toString(keywords));
        
    	String crawlDataPath = Configuration.CRAWLS_DIR + collectionName;
    	Query<CrawlJob> q = dao.createQuery().filter("collection", collectionName);
        List<CrawlJob> requestsWithSameName = dao.find(q).asList();
        if (isNew && (new File(crawlDataPath).exists() || requestsWithSameName.size() > 0)) {
        	_logger.error("The collection " + collectionName + " already exists. Choose a different name or mark not new");
            throw new Exception("The collection " + collectionName + " already exists. Choose a different name or mark not new");
        }
        else {
        	CrawlJob job = new CrawlJob(crawlDataPath, collectionName, keywords, isNew);
        	dao.save(job);
        
        	tryLaunch();
        
        	return job;
        }
    }

    // submit geo-location crawl job
    public synchronized CrawlJob submit(String collectionName, double lon_min, double lat_min, double lon_max, double lat_max) throws Exception {
    	_logger.info("CRAWL: submit " + collectionName + " coordinates [<" + lon_min + ", " + lat_min + ">, <" + lon_max + ", " + lat_max + ">]");
        CrawlJob job = new CrawlJob(collectionName, lon_min, lat_min, lon_max, lat_max);
        dao.save(job);
        
        tryLaunch();
        return job;
    }

    public CrawlJob stop(String id) throws Exception {
        return cancel(id, false);
    }

    public CrawlJob kill(String id) throws Exception {
        return cancel(id, true);
    }

    private synchronized CrawlJob cancel(String id, boolean immediately) throws Exception {
        CrawlJob req = getCrawlRequest(id);
        if(req == null) {
        	_logger.error("Cannot find job with id=" + id);
        }
        
        _logger.info("Cancel crawl for id " + id + " (" + req.getCollection() + ")");
        if(CrawlJob.STATE.RUNNING == req.getState() || CrawlJob.STATE.WAITING == req.getState() || CrawlJob.STATE.STARTING == req.getState()) {
            req.setState(immediately ? CrawlJob.STATE.KILLING : CrawlJob.STATE.STOPPING);
            req.setLastStateChange(new Date());
            dao.save(req);
            
            if (req.getKeywords().isEmpty()) {
                cancelGeoCrawl(req.getCollection());
            }
            else {
            	RevealAgent agent = agents.remove(req.getCollection());
            	if(agent != null) {
            		agent.stop();
            	}
            	else {
            		_logger.error("Agent is null. Cannot stop it for " + req.getCollection());
            	}
            }
        }
        else if(CrawlJob.STATE.STOPPING == req.getState() || CrawlJob.STATE.KILLING == req.getState()) {
        	_logger.info("Crawl state of " + req.getState() + " is " + req.getState()+". It will stop eventually.");
        }
        else {
        	_logger.info("Crawl state of " + req.getState() + " is " + req.getState()+". You can only stop RUNNING / WAITING / STARTING crawls.");
        }
        
        return req;
    }

    public synchronized CrawlJob delete(String id) throws Exception {
        CrawlJob req = getCrawlRequest(id);
        if(req == null) {
        	_logger.info("CrawlJob with " + id + " is null. Cannot delete.");
        	 return req;
        }
        
        _logger.info("Delete crawl for id " + id + " (" + req.getCollection() + ")");
        if (req.getState() == CrawlJob.STATE.FINISHED || req.getState() == CrawlJob.STATE.DELETING) {
            
        	req.setState(CrawlJob.STATE.DELETING);
            req.setLastStateChange(new Date());
            dao.save(req);
            
            //Unload index from memory and delete it
            VisualIndexClient vIndexClient = new VisualIndexClient(indexServiceHost, req.getCollection());
        	boolean indexDeleted = vIndexClient.deleteCollection();
        	if(!indexDeleted) {
        		_logger.error("Visual index of " + req.getCollection() + " failed to be deleted.");
        	}
        	
            try {	
                //Delete the crawl folders
                FileUtils.deleteDirectory(new File(req.getCrawlDataPath()));
            }
            catch(Exception e) {
            	_logger.error("Exception during crawl deletion of " + req.getCollection() + " => " + e.getMessage(), e);
            }
            
            //Delete the request from the request DB
            dao.delete(req);
            MorphiaManager.getDB(req.getCollection()).drop();
        } 
        else if(CrawlJob.STATE.RUNNING == req.getState() || CrawlJob.STATE.WAITING == req.getState() || CrawlJob.STATE.STARTING == req.getState()) {
        	kill(id);
        	delete(id);
        }
        else if(CrawlJob.STATE.KILLING == req.getState() || CrawlJob.STATE.STOPPING == req.getState()) {
        	while(CrawlJob.STATE.KILLING == req.getState() || CrawlJob.STATE.STOPPING == req.getState()) {
        		Thread.sleep(5000);
        		req = getCrawlRequest(id);
        	}
        	delete(id);
        }
        else {
        	_logger.error("Collection: " + req.getCollection() + " failed to be deleted. State " + req.getState() + ". You can only delete RUNNING, FINISHED or DELETING crawls");
        }
        
        return req;
    }

    private void tryLaunch() throws Exception {
        List<CrawlJob> runningCrawls = getRunningCrawls();
        _logger.info("Running crawls list size: " + runningCrawls.size());
        if (runningCrawls.size() >= Configuration.NUM_CRAWLS) {
        	_logger.info("Cannot run more crawls. Number of crawls reached: " + Configuration.NUM_CRAWLS);
            return;
        }
        
        List<CrawlJob> waitingCrawls = getWaitingCrawls();
        if (waitingCrawls.isEmpty()) {
        	_logger.info("There are no waiting crawls to start!");
            return;
        }
        
        _logger.info(waitingCrawls.size() + " crawls waiting to start.");
        
        CrawlJob req = waitingCrawls.get(0);
        _logger.info("Start " + req.getCollection());
        
        req.setState(CrawlJob.STATE.STARTING);
        dao.save(req);
        
        startCrawl(req);
    }

    private void startWaitingCrawls() {
    	List<CrawlJob> crawlsToStart = getWaitingCrawls();
   	 
   	 	int running = 0;
   	 	for(CrawlJob job : crawlsToStart) {
   	 		try {
   	 			if(running < Configuration.NUM_CRAWLS) {
   	 				_logger.info("Run " + job.getCollection());
   	 				job.setState(CrawlJob.STATE.STARTING);
   	 				dao.save(job);
   	        	
   	 				startCrawl(job);
   	 				running++;
   	 			}
   	 			else {
   	 				_logger.info("Cannot run " + job.getCollection() + ". Number of crawls reached.");
   	 				job.setState(CrawlJob.STATE.WAITING);
   	 				dao.save(job);
   	 			}
   	 		} catch (Exception e) {
				_logger.error("Failed to start " + job.getCollection());
			}
   	 	}
    }
    
    private void startRunningCrawlsAtStartup() {
    	List<CrawlJob> crawlsToStart = getRunningCrawls();
   	 
   	 	int running = 0;
   	 	for(CrawlJob job : crawlsToStart) {
   	 		try {
   	 			if(running < Configuration.NUM_CRAWLS) {
   	 				_logger.info("Run " + job.getCollection());
   	 				job.setState(CrawlJob.STATE.STARTING);
   	 				dao.save(job);
   	        	
   	 				startCrawl(job);
   	 				running++;
   	 			}
   	 			else {
   	 				_logger.info("Cannot run " + job.getCollection() + ". Number of crawls reached.");
   	 				job.setState(CrawlJob.STATE.WAITING);
   	 				dao.save(job);
   	 			}
   	 		} catch (Exception e) {
				_logger.error("Failed to start " + job.getCollection());
			}
   	 	}
    }
    
    private void deleteAndStopCrawlsAtStartup() {
    	List<CrawlJob> crawlsToDelete = getDeletingCrawls();
    	for(CrawlJob job : crawlsToDelete) {
    		try {
    			this.delete(job.getId());
			} catch (Exception e) {
				_logger.error("Failed to delete " + job.getCollection());
			}
    	}
    	 
    	List<CrawlJob> crawlsToStop = getStoppingCrawls();
    	for(CrawlJob job : crawlsToStop) {
    		try {
    			this.kill(job.getId());
    		} catch (Exception e) {
    			_logger.error("Failed to stop " + job.getCollection());
    		}
    	}
    }
    
    private void startCrawl(CrawlJob req) throws Exception {
        _logger.info("Start crawl " + req.getCollection() + " " + req.getState());
        if (req.getKeywords().isEmpty()) {
            geoCrawlerMap.put(req.getCollection(), new GeoCrawler(req, streamManager));
        } 
        else {
            RevealAgent agent = new RevealAgent("127.0.0.1", 9999, req, streamManager);
            executorService.execute(agent);
            agents.put(req.getCollection(), agent);
            
            _logger.info("Reveal agent started for collection " + req.getCollection());
        }
    }

    private void cancelGeoCrawl(String name) throws StreamException {
        if (geoCrawlerMap.get(name) != null) {
            geoCrawlerMap.get(name).stop();
            geoCrawlerMap.remove(name);
        }
    }
    
    
    ///////////////////////////////////////////////////////
    ///////////////// DB STUFF ///////////////////////////
    //////////////////////////////////////////////////////

    private CrawlJob getCrawlRequest(String id) {
        return dao.findOne("_id", id);
    }

    private List<CrawlJob> getRunningCrawls() {
        Query<CrawlJob> q = dao.createQuery();
        q.or(
        		q.criteria("requestState").equal(CrawlJob.STATE.RUNNING),
        		q.criteria("requestState").equal(CrawlJob.STATE.STARTING)
        );
        return q.asList();
    }

    private List<CrawlJob> getStoppingCrawls() {
    	Query<CrawlJob> q = dao.createQuery();
        q.or(
        		q.criteria("requestState").equal(CrawlJob.STATE.STOPPING),
                q.criteria("requestState").equal(CrawlJob.STATE.KILLING)
        	);
                
        return dao.find(q).asList();
    }
    
    private List<CrawlJob> getDeletingCrawls() {
        return dao.getDatastore().find(CrawlJob.class).filter("requestState", CrawlJob.STATE.DELETING).asList();
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
                q.criteria("requestState").equal(CrawlJob.STATE.KILLING),
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
            status.image = imageDAO.getItems(1, 0).get(0);
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
            case KILLING:
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
    
    /////////////////////////////////////////////////////
    ///////////////// POLLING ///////////////////////////
    ////////////////////////////////////////////////////
    public class Poller implements Runnable {
        
    	final ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
        Future<?> future = null;

        @Override
        public void run() {
            try {
            	startWaitingCrawls();
            } catch (Exception ex) {
            	_logger.error(ex);
            }
        }

        public void startPolling() {
        	future = exec.scheduleAtFixedRate(this, 10, 60, TimeUnit.SECONDS);
        }

        public void stopPolling() {
        	exec.shutdownNow();
        	if (future != null && !future.isDone()) {
               future.cancel(true);
            }
        }

    }
}
