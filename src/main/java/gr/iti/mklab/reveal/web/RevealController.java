package gr.iti.mklab.reveal.web;

import com.google.gson.Gson;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import gr.iti.mklab.reveal.clustering.ClusteringCallable;
import gr.iti.mklab.reveal.summarization.MediaSummarizer;
import gr.iti.mklab.reveal.summarization.RankedImage;
import gr.iti.mklab.reveal.util.Configuration;
import gr.iti.mklab.reveal.crawler.CrawlQueueController;
import gr.iti.mklab.reveal.entities.NEandRECallable;
import gr.iti.mklab.reveal.visual.JsonResultSet;
import gr.iti.mklab.reveal.visual.VisualFeatureExtractor;
import gr.iti.mklab.reveal.visual.VisualIndexClient;
import gr.iti.mklab.reveal.web.Responses.SimilarityResponse;
import gr.iti.mklab.reveal.web.Responses.SummaryResponse;
import gr.iti.mklab.simmo.core.Annotation;
import gr.iti.mklab.simmo.core.Association;
import gr.iti.mklab.simmo.core.UserAccount;
import gr.iti.mklab.simmo.core.annotations.DisturbingScore;
import gr.iti.mklab.simmo.core.annotations.NamedEntity;
import gr.iti.mklab.simmo.core.associations.TextualRelation;
import gr.iti.mklab.simmo.core.cluster.Cluster;
import gr.iti.mklab.simmo.core.items.Image;
import gr.iti.mklab.simmo.core.items.Media;
import gr.iti.mklab.simmo.core.items.Video;
import gr.iti.mklab.simmo.core.jobs.CrawlJob;
import gr.iti.mklab.simmo.core.morphia.AssociationDAO;
import gr.iti.mklab.simmo.core.morphia.MediaDAO;
import gr.iti.mklab.simmo.core.morphia.MorphiaManager;

import org.apache.log4j.Logger;
import org.mongodb.morphia.dao.BasicDAO;
import org.mongodb.morphia.dao.DAO;
import org.mongodb.morphia.query.Query;
import org.mongodb.morphia.query.QueryResults;
import org.mongodb.morphia.query.UpdateOperations;
import org.mongodb.morphia.query.UpdateResults;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.annotation.PreDestroy;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


@Controller
@RequestMapping("/mmapi")
public class RevealController {

    private Logger _logger = Logger.getLogger(RevealController.class);
    
    private CrawlQueueController crawlControler;

    private ExecutorService executorService = Executors.newFixedThreadPool(8);
    
    public RevealController() throws Exception {
    	
        Configuration.load(getClass().getResourceAsStream("/remote.properties"));
        
        // initialize mongodb
        MorphiaManager.setup(Configuration.MONGO_HOST);
        
        // initialize visual feature extractor
        VisualFeatureExtractor.init(true);
        
        crawlControler = new CrawlQueueController();
    }

    @PreDestroy
    public void cleanUp() throws Exception {
    	_logger.info("Spring Container destroy");
    	executorService.shutdownNow();
    	
        MorphiaManager.tearDown();
        if (crawlControler != null) {
        	crawlControler.shutdown();
        }
    }

    ////////////////////////////////////////////////////////
    ///////// NAMED ENTITIES     ///////////////////////////
    ///////////////////////////////////////////////////////

    /**
     * Extracts named entities from all the collection items, ranks them by frequency and stores them in a new table
     *
     * @param collection
     * @return
     * @throws Exception
     */
    @RequestMapping(value = "/media/{collection}/extract", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public String extractEntities(@PathVariable(value = "collection") String collection) throws Exception {
    	executorService.submit(new NEandRECallable(collection));
        return "{ \"status\" : \"Extracting entities for " + collection + "\"}";
    }

    /**
     * Returns the ranked entities for the specified collection
     *
     * @param collection
     * @return
     * @throws Exception
     */
    @RequestMapping(value = "/media/{collection}/entities", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public List<NamedEntity> entitiesForCollection(
    		@PathVariable(value = "collection") String collection) throws Exception {
        
    	DAO<NamedEntity, String> rankedEntities = new BasicDAO<>(NamedEntity.class, MorphiaManager.getMongoClient(), MorphiaManager.getMorphia(), MorphiaManager.getDB(collection).getName());
        int numberofEntitiesToReturn = 300;
        if(rankedEntities != null && rankedEntities.count() > 0) {
        	List<NamedEntity> list = rankedEntities.find().asList();
        	if(list == null) {
        		return new ArrayList<NamedEntity>();
        	}
        	else {
        		return list.subList(0, rankedEntities.count() > numberofEntitiesToReturn ? numberofEntitiesToReturn : (int)rankedEntities.count());
        	}
        }
        else {
        	return new ArrayList<NamedEntity>();
        }
        
    }

    /**
     * Returns the textual relations among named entities
     *
     * @param collection
     * @return
     * @throws Exception
     */
    @RequestMapping(value = "/media/{collection}/relations", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public List<TextualRelation> relationsForCollection(@PathVariable(value = "collection") String collection) throws Exception {
        AssociationDAO associationDAO = new AssociationDAO(collection);
        List<Association> assList = associationDAO.getDatastore().find(Association.class).disableValidation().filter("className", TextualRelation.class.getName()).
                limit(300).asList();
        List<TextualRelation> trlist = new ArrayList<>(assList.size());
        assList.stream().forEach(association ->
                        trlist.add(((TextualRelation) association))
        );
        return trlist;
    }

    ////////////////////////////////////////////////////////
    /////////////////// CRAWLER ///////////////////////////
    ///////////////////////////////////////////////////////

    @RequestMapping(value = "/crawls/add", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public CrawlJob submitCrawlingJob(@RequestParam(value = "text", required = true) String json) throws RevealException {
        try {
            Gson gson = new Gson();
            CrawlPostRequest request = gson.fromJson(json, CrawlPostRequest.class);
            
            if (request.getKeywords() == null || request.getKeywords().isEmpty()) {
                return crawlControler.submit(request.getCollection(), request.getLon_min(), request.getLat_min(), request.getLon_max(), request.getLat_max());
            }
            else {
                return crawlControler.submit(request.isNew(), request.getCollection(), request.getKeywords());
            }
        } catch (Exception ex) {
        	_logger.error("Exception during crawl submission: " + ex.getMessage());
            throw new RevealException(ex.getMessage(), ex);
        }
    }

    /**
     * Stops all running threads and deletes all data related to this collection
     * @param id
     * @return
     * @throws RevealException
     */
    @RequestMapping(value = "/crawls/{id}/delete", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public boolean deleteCrawlingJob(@PathVariable(value = "id") String id) throws RevealException {
        try {
        	crawlControler.delete(id);
            return true;
        } catch (Exception ex) {
        	_logger.error("Exception during crawl delete: " + ex.getMessage());
            throw new RevealException("Error when deleting", ex);
        }
    }

    /**
     * Stops the crawling, waits until the indexing of all remaining images finishes
     * and then launches the clustering and entity extraction
     * @param id
     * @return
     * @throws RevealException
     */
    @RequestMapping(value = "/crawls/{id}/stop", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public CrawlJob stopCrawlingJob(@PathVariable(value = "id") String id) throws RevealException {
        try {
            return crawlControler.stop(id);
        } catch (Exception ex) {
        	_logger.error("Exception during crawl stopping: " + ex.getMessage());
            throw new RevealException("Error when stopping", ex);
        }
    }

    /**
     * Stops the crawling, stops the indexing and launches the clustering and entity extraction
     * @param id
     * @return
     * @throws RevealException
     */
    @RequestMapping(value = "/crawls/{id}/kill", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public CrawlJob killCrawlingJob(@PathVariable(value = "id") String id) throws RevealException {
        try {
        	return crawlControler.kill(id);
        } catch (Exception ex) {
        	_logger.error("Error when killing id=" + id + ": " + ex.getMessage());
            throw new RevealException("Error when killing " + id, ex);
        }
    }

    @RequestMapping(value = "/crawls/{id}/status", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public Responses.CrawlStatus getCrawlingJobStatus(@PathVariable(value = "id") String id) {
        return crawlControler.getStatus(id);
    }

    /**
     * @return a list of CrawlRequests that are either RUNNING or WAITING
     */
    @RequestMapping(value = "/crawls/status", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public List<Responses.CrawlStatus> getCrawlerStatus() {
        List<Responses.CrawlStatus> s = crawlControler.getActiveCrawls();
        return s;
    }

    ////////////////////////////////////////////////////////
    ///////// COLLECTIONS INDEXING             /////////////
    ///////////////////////////////////////////////////////

    /**
     * Adds a collection with the specified name
     * <p/>
     * Example: http://localhost:8090/reveal/mmapi/collections/add?name=revealsample
     *
     * @param name
     * @return
     */
    @RequestMapping(value = "/collections/add", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public Responses.IndexResponse collectionsAdd(
            @RequestParam(value = "name", required = true) String name,
            @RequestParam(value = "size", required = false, defaultValue = "100000") int numVectors) {
        try {
        	String indexServiceHost = "http://" + Configuration.INDEX_SERVICE_HOST + ":8080/VisualIndexService";
    		VisualIndexClient vIndexClient = new VisualIndexClient(indexServiceHost, name);
    		
    		vIndexClient.createCollection();
    		
            return new Responses.IndexResponse();
        } catch (Exception ex) {
            return new Responses.IndexResponse(false, ex.toString());
        }
    }

    ////////////////////////////////////////////////////////
    ///////// MEDIA NEW API WITH SIMMO FRAMEWORK ///////////
    ///////////////////////////////////////////////////////

    @RequestMapping(value = "/media/{collection}", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public Responses.MediaResponse mediaItems(@RequestParam(value = "count", required = false, defaultValue = "10") int count,
                                           		@RequestParam(value = "offset", required = false, defaultValue = "0") int offset,
                                                @RequestParam(value = "type", required = false) String type,
                                                @PathVariable(value = "collection") String collection) {
    	
        Responses.MediaResponse response = new Responses.MediaResponse();
        
        if (type == null || type.equalsIgnoreCase("image")) {
            MediaDAO<Image> imageDAO = new MediaDAO<>(Image.class, collection);
            response.images = imageDAO.getItems(count, offset);
            response.numImages = imageDAO.count();
        }
        
        if (type == null || type.equalsIgnoreCase("video")) {
            MediaDAO<Video> videoDAO = new MediaDAO<>(Video.class, collection);
            response.videos = videoDAO.getItems(count, offset);
            response.numVideos = videoDAO.count();
        }
        
        response.offset = offset;
        return response;
    }

    @RequestMapping(value = "/media/{collection}/{id}", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public Media mediaItemById(@PathVariable(value = "collection") String collection,
                                 @PathVariable("id") String id) {
        Media result;
        MediaDAO<Image> imageDAO = new MediaDAO<>(Image.class, collection);
        result = imageDAO.get(id);
        if (result == null) {
            MediaDAO<Video> videoDAO = new MediaDAO<>(Video.class, collection);
            result = videoDAO.get(id);
        }
        
        return result;
    }

    @RequestMapping(value = "/media/{collection}/search", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public Responses.MediaResponse mediaItemsSearch(
            @PathVariable(value = "collection") String collection,
            @RequestParam(value = "user", required = false) String username,
            @RequestParam(value = "date", required = false, defaultValue = "-1") long date,
            @RequestParam(value = "w", required = false, defaultValue = "0") int w,
            @RequestParam(value = "h", required = false, defaultValue = "0") int h,
            @RequestParam(value = "count", required = false, defaultValue = "10") int count,
            @RequestParam(value = "offset", required = false, defaultValue = "0") int offset,
            @RequestParam(value = "query", required = false) String query,
            @RequestParam(value = "type", required = false) String type,
            @RequestParam(value = "sources", required = false) String sources) {

        Responses.MediaResponse response = new Responses.MediaResponse();
        UserAccount account = null;
        if (username != null) {
            DAO<UserAccount, String> userDAO = new BasicDAO<>(UserAccount.class, MorphiaManager.getMongoClient(), MorphiaManager.getMorphia(), MorphiaManager.getDB(collection).getName());
            account = userDAO.findOne("username", username);
        }
        List<String> sourcesList = null;
        if (sources != null) {
            sourcesList = Arrays.asList(sources.split(","));
        }

        if (type == null || type.equalsIgnoreCase("image")) {
            MediaDAO<Image> imageDAO = new MediaDAO<>(Image.class, collection);
            response.images = imageDAO.search("crawlDate", new Date(date), w, h, count, offset, account, query, sourcesList);
            response.numImages = imageDAO.count();
        }
        
        if (type == null || type.equalsIgnoreCase("video")) {
            MediaDAO<Video> videoDAO = new MediaDAO<>(Video.class, collection);
            response.videos = videoDAO.search("crawlDate", new Date(date), w, h, count, offset, account, query, sourcesList);
            response.numVideos = videoDAO.count();
        }
        response.offset = offset;
        return response;
    }

    @RequestMapping(value = "/media/{collection}/similar", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public List<Responses.SimilarityResponse> findSimilarImages(@PathVariable(value = "collection") String collectionName,
                                                              	@RequestParam(value = "imageurl", required = true) String imageUrl,
                                                              	@RequestParam(value = "offset", required = false, defaultValue = "0") int offset,
                                                              	@RequestParam(value = "count", required = false, defaultValue = "50") int count,
                                                              	@RequestParam(value = "threshold", required = false, defaultValue = "0.6") double threshold) {
        try {
            MediaDAO<Image> imageDAO = new MediaDAO<>(Image.class, collectionName);
            MediaDAO<Video> videoDAO = new MediaDAO<>(Video.class, collectionName);

            VisualIndexClient handler = new VisualIndexClient("http://" + Configuration.INDEX_SERVICE_HOST + ":8080/VisualIndexService", collectionName);
               
            double[] vector = VisualFeatureExtractor.vectorizeImageFromUrl(imageUrl);
           _logger.info("Collection: " + collectionName + ". Find similar images for: " + imageUrl + ".  Vector length: " + vector.length);
           
            JsonResultSet similar = handler.getSimilarImages(vector, threshold);
            _logger.info(similar.getResults().size() + " similar images found for " + imageUrl);
            		
            List<JsonResultSet.JsonResult> temp = similar.getResults();
            List<SimilarityResponse> simList = new ArrayList<Responses.SimilarityResponse>(temp.size());
            for (JsonResultSet.JsonResult res : temp) {
            	Media found = imageDAO.get(res.getId());
                if (found != null) {
                	simList.add(new Responses.SimilarityResponse(found, res.getRank()));
                }
                else {
                	 found = videoDAO.get(res.getId());
                     if (found != null) {
                     	simList.add(new Responses.SimilarityResponse(found, res.getRank()));
                     }
                     else {
                    	 _logger.error("Couldn't find " + res.getId() + " in " + collectionName);
                     }
                }
            }
            
            if (simList.size() < count) {
                return simList;
            }
            else {
                return simList.subList(offset, offset + count);
            }

        } catch (Exception e) {
            _logger.error("Exception for similar retrieval of " + imageUrl + ": " + e.getMessage());
            return new ArrayList<>();
        }
    }

    ////////////////////////////////////////////////////////
    ////////// C L U S T E R I N G /////////////////////////
    ///////////////////////////////////////////////////////

    @RequestMapping(value = "/media/{collection}/cluster", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public String clusterCommandIncremental(@PathVariable(value = "collection") String collection,
                                            @RequestParam(value = "eps", required = true, defaultValue = "1.2") double eps,
                                            @RequestParam(value = "minpoints", required = true, defaultValue = "2") int minpoints,

                                            @RequestParam(value = "count", required = true, defaultValue = "1000") int count) throws RevealException {
    	executorService.submit(new ClusteringCallable(collection, count, eps, minpoints));
        return "Clustering command submitted for collection " + collection;
    }

    @RequestMapping(value = "/clusters/{collection}", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public List<ClusterReduced> getClusters(@PathVariable(value = "collection") String collection,
                                            @RequestParam(value = "offset", required = false, defaultValue = "0") int offset,
                                            @RequestParam(value = "count", required = false, defaultValue = "50") int count) {
    
    	MediaDAO<Image> imageDAO = new MediaDAO<>(Image.class, collection);
    	MediaDAO<Video> videoDAO = new MediaDAO<>(Video.class, collection);
    	
    	DAO<gr.iti.mklab.simmo.core.cluster.Cluster, String> clusterDAO = new BasicDAO<>(
    			gr.iti.mklab.simmo.core.cluster.Cluster.class, 
    			MorphiaManager.getMongoClient(), 
    			MorphiaManager.getMorphia(), 
    			MorphiaManager.getDB(collection).getName());
    	
    	Query<Cluster> query = clusterDAO.createQuery().filter("size >", 1).order("-size").offset(offset).limit(count);
		List<Cluster> clustersResult = clusterDAO.find(query).asList();
        List<ClusterReduced> minimalList = new ArrayList<ClusterReduced>();
        Iterator<Cluster> it = clustersResult.iterator();
        while (it.hasNext()) {
        	try {
        		gr.iti.mklab.simmo.core.cluster.Cluster cluster = it.next();
        		ClusterReduced cr = new ClusterReduced();
        		
        		cr.id = cluster.getId();
        		cr.members = cluster.getSize();
        		
        		if(cluster.getCentroid() == null) {
        			Map<String, String> centroids = cluster.getCentroids();
        			if(centroids != null) {
        				for(String centroidId : centroids.keySet()) {
        					String type = centroids.get(centroidId);
        					if(type.equals("image")) {
        						cr.item = imageDAO.get(centroidId);
        					}
        					else {
        						cr.item = videoDAO.get(centroidId);
        					}
        				}
        			}
        			
        			if(cr.item == null && cr.members > 0) {
        				cr.item = (Media) cluster.getMembers().get(0);	
        			}
        			
        		}
        		else {
        			cr.item = (Media) cluster.getCentroid();
        		}
        		
        		minimalList.add(cr);
        	}
        	catch(Exception e) {
        		_logger.info(e.getMessage());
        	}
        }
        
        return minimalList;
    }

    @RequestMapping(value = "/clusters/{collection}/{id}", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public gr.iti.mklab.simmo.core.cluster.Cluster getCluster(@PathVariable(value = "collection") String collection,
                                                              @PathVariable(value = "id") String id,
                                                              @RequestParam(value = "offset", required = false, defaultValue = "0") int offset,
                                                              @RequestParam(value = "count", required = false, defaultValue = "50") int count) {
        DAO<gr.iti.mklab.simmo.core.cluster.Cluster, String> clusterDAO = new BasicDAO<>(gr.iti.mklab.simmo.core.cluster.Cluster.class, MorphiaManager.getMongoClient(), MorphiaManager.getMorphia(), MorphiaManager.getDB(collection).getName());
        gr.iti.mklab.simmo.core.cluster.Cluster c = clusterDAO.get(id);
        if (offset < c.getMembers().size()) {
            c.setMembers(c.getMembers().subList(offset, c.getMembers().size() < offset + count ? c.getMembers().size() : offset + count));
        }
        else {
            c = new gr.iti.mklab.simmo.core.cluster.Cluster();
        }
        return c;
    }

    class ClusterReduced {
        public String id;
        public int members;
        public Media item;
    }

    ////////////////////////////////////////////////////////
    ////////// S U M M A R I Z A T I O N //////////////////
    ///////////////////////////////////////////////////////
    
    private ExecutorService summarizationExecutor = Executors.newSingleThreadExecutor();
    private Map<String, Future<List<RankedImage>>> futures = new HashMap<String, Future<List<RankedImage>>>();
    
    /**
     * Execute summarization procedure
     *
     * @param collection
     * @return
     * @throws Exception
     */
    @RequestMapping(value = "/media/{collection}/summarize", 
    		method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public String summarizationForCollection(
    		@PathVariable(value = "collection") String collection,
    		@RequestParam(value = "similarityCuttof", required = false, defaultValue = "0.2") double similarityCuttof, 
    		@RequestParam(value = "visualCuttof", required = false, defaultValue = "0.2") double visualCuttof,
    		@RequestParam(value = "randomJumpWeight", required = false, defaultValue = "0.75") double randomJumpWeight,
    		@RequestParam(value = "scanMu", required = false, defaultValue = "3") int scanMu,
    		@RequestParam(value = "scanEpsilon", required = false, defaultValue = "0.65") double scanEpsilon)
    				throws Exception {
    	
    	Future<List<RankedImage>> response = futures.get(collection);
    	if(response == null) {
    		MediaSummarizer summarizer = new MediaSummarizer(collection, similarityCuttof, visualCuttof, 
    				randomJumpWeight, scanMu, scanEpsilon);
    		
        	response = summarizationExecutor.submit(summarizer);
        	if(response != null) {
        		futures.put(collection, response);
        		return "Summarization command is submitted.";
        	}
        	else {
        		return "Summarization command failed to be submitted.";
        	}
    	}
    	else {
    		return "Summarization task has already been submitted.";
    	}
    }
    
    @RequestMapping(value = "/summary/{collection}", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public Responses.SummaryResponse getSummary(@PathVariable(value = "collection") String collection,
                                            @RequestParam(value = "count", required = false, defaultValue = "50") int count) {
        
    	Future<List<RankedImage>> response = futures.get(collection);
    	if(response == null) {
    		DAO<RankedImage, String> rankedImagesDAO = new BasicDAO<RankedImage, String>(
    				RankedImage.class, 
    				MorphiaManager.getMongoClient(), 
    				MorphiaManager.getMorphia(), 
    				MorphiaManager.getDB(collection).getName()
    			);
    		
    		Query<RankedImage> q = rankedImagesDAO.createQuery()
    				.order("-score")
    				.limit(count);
    		
    		QueryResults<RankedImage> rankedImages = rankedImagesDAO.find(q);
    		List<RankedImage> summary = rankedImages.asList();
    		
    		SummaryResponse sr = new SummaryResponse("finnished");
    		if(summary == null | summary.isEmpty()) {
        		sr.setStatus("uncommitted task");
    		}
    		
    		sr.setSummary(summary);

    		return sr;
    	}
    	
    	if(response.isCancelled()) {
    		return new SummaryResponse("cancelled");
    	}
    	
    	if(!response.isDone()) {
    		return new SummaryResponse("running");
    	}
    	else {
    		try {
    			SummaryResponse sr = new SummaryResponse("finnished");
				List<RankedImage> summary = response.get();
				sr.setSummary(summary);
				
				futures.remove(collection);
				
				return sr;
			} catch (InterruptedException | ExecutionException e) {
				return new SummaryResponse("failed");
			}
    	}
    	
    }
    
    @RequestMapping(value = "/media/update/disturbing", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public String updateDisturbingValue(
    		@RequestParam(value = "collection", required = true) String collection,
    		@RequestParam(value = "url", required = true) String url,
    		@RequestParam(value = "id", required = false) String id,
    		@RequestParam(value = "score", required = true) double score,
    		@RequestParam(value = "type", required = true) String type) throws RevealException {

    	DBObject dbObj = new BasicDBObject();
    	dbObj.put("collection", collection);
    	dbObj.put("score", score);
    	dbObj.put("url", url);
    	dbObj.put("type", type);
    	dbObj.put("id", id);
    	
    	if(score < 0 || score > 1) {
    		dbObj.put("action", "Media cannot be updated. Score is not in proper range!");
    		return dbObj.toString();	
    	}
    	
    	Annotation scoreAnnotation = new DisturbingScore(score);
    	if(type.equals("image")) {
    		MediaDAO<Image> imageDAO = new MediaDAO<>(Image.class, collection);
    		Query<Image> mq = imageDAO.createQuery();
    		if(id != null && !id.equals(null) && !id.equals("")) {
    			mq.filter("_id", id);
    		}
    		else {
    			mq.filter("url", url);
    		}
    		
    		dbObj.put("q", mq.toString());
    		UpdateOperations<Image> mOps = imageDAO.createUpdateOperations().add("annotations", scoreAnnotation, true);
    		dbObj.put("ops", mOps.toString());
    		
    		try {
    			UpdateResults r = imageDAO.update(mq, mOps);
    			dbObj.put("action", r.getUpdatedCount() + " updated");
        	}
        	catch(Exception e) { 
        		dbObj.put("exception", e.getMessage());
        	}
    		
    	}
    	else if(type.equals("video")) {
    		MediaDAO<Video> videoDAO = new MediaDAO<>(Video.class, collection);
        	Query<Video> vq = videoDAO.createQuery();
    		if(id != null && !id.equals(null) && !id.equals("")) {
    			vq.filter("_id", id);
    		}
    		else {
    			vq.filter("url", url);
    		}
    		
        	dbObj.put("q", vq.toString());
        	UpdateOperations<Video> vOps = videoDAO.createUpdateOperations().add("annotations", scoreAnnotation);
        	dbObj.put("ops", vOps.toString());
        	
        	try {
        		UpdateResults r = videoDAO.update(vq, vOps);
        		dbObj.put("action", r.getUpdatedCount() + " updated");
        	}
        	catch(Exception e) { 
        		dbObj.put("exception", e.getMessage());
        	}
    	}
    	
    	return dbObj.toString();
    }
    
    ////////////////////////////////////////////////////////
    ///////// EXCEPTION HANDLING ///////////////////////////
    ///////////////////////////////////////////////////////

    @ResponseStatus(value = HttpStatus.INTERNAL_SERVER_ERROR)
    @ExceptionHandler(RevealException.class)
    @ResponseBody
    public RevealException handleCustomException(RevealException ex) {
        return ex;
    }
}