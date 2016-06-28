package gr.iti.mklab.reveal.crawler;

import gr.iti.mklab.reveal.rabbitmq.RabbitMQPublisher;
import gr.iti.mklab.reveal.util.Configuration;
import gr.iti.mklab.reveal.util.DisturbingDetectorClient;
import gr.iti.mklab.reveal.visual.MediaCallable;
import gr.iti.mklab.reveal.visual.MediaCallableResult;
import gr.iti.mklab.reveal.visual.VisualIndexer;
import gr.iti.mklab.reveal.visual.VisualIndexerFactory;
import gr.iti.mklab.simmo.core.annotations.lowleveldescriptors.LocalDescriptors;
import gr.iti.mklab.simmo.core.documents.Webpage;
import gr.iti.mklab.simmo.core.items.Image;
import gr.iti.mklab.simmo.core.items.Media;
import gr.iti.mklab.simmo.core.items.Video;
import gr.iti.mklab.simmo.core.morphia.MediaDAO;
import gr.iti.mklab.simmo.core.morphia.MorphiaManager;
import gr.iti.mklab.simmo.core.morphia.ObjectDAO;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.mongodb.morphia.query.Query;
import org.mongodb.morphia.query.UpdateOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A runnable that indexes all non indexed images found in the specified collection
 * and waits if there are no new images or videos to index
 *
 * @author kandreadou
 */
public class IndexingRunner implements Runnable {

    private final static Logger LOGGER = LoggerFactory.getLogger(IndexingRunner.class);
    
    private final static int INDEXING_PERIOD = 30 * 1000;
    private final static int STEP = 100;
    
    private VisualIndexer _indexer;
    
    private RabbitMQPublisher _publisher;
    private MediaDAO<Image> imageDAO;
    private MediaDAO<Video> videoDAO;
    private ObjectDAO<Webpage> pageDAO;
    
    private LocalDescriptors ld;
    private boolean isRunning = true;
    private boolean shouldStop = false;
    private boolean listsWereEmptyOnce = false;
    private final String collection;
    
    private ExecutorService executor;
	private CompletionService<MediaCallableResult> pool;
	private int numPendingTasks;
	private final int maxNumPendingTasks;
	private int NUM_THREADS = 10;

    public IndexingRunner(String collection) throws ExecutionException, IOException {
        
    	LOGGER.info("Creating IndexingRunner for collection " + collection);
        this.collection = collection;
        _indexer = VisualIndexerFactory.getVisualIndexer(collection);
        LOGGER.info("After creating the indexer ");
       
        if (Configuration.PUBLISH_RABBITMQ) {
            _publisher = new RabbitMQPublisher("localhost", collection);
        }
        
        if(Configuration.DISTURBING_DETECTOR_HOST != null) {
        	DisturbingDetectorClient.initialize(Configuration.DISTURBING_DETECTOR_HOST);
        }
        
        imageDAO = new MediaDAO<>(Image.class, collection);
        videoDAO = new MediaDAO<>(Video.class, collection);
        pageDAO = new ObjectDAO<>(Webpage.class, collection);
        
        ld = new LocalDescriptors();
        ld.setDescriptorType(LocalDescriptors.DESCRIPTOR_TYPE.SURF);
        ld.setFeatureEncoding(LocalDescriptors.FEATURE_ENCODING.Vlad);
        ld.setNumberOfFeatures(1024);
        ld.setFeatureEncodingLibrary("multimedia-indexing");
        
        executor = Executors.newFixedThreadPool(NUM_THREADS);
		pool = new ExecutorCompletionService<MediaCallableResult>(executor);
		
		numPendingTasks = 0;
		maxNumPendingTasks = NUM_THREADS * 10;
        System.out.println("End of constructor ");
    }

    @Override
    public void run() {
    			
        System.out.println("Indexing runner run");
        int submittedCounter = 0;
		int completedCounter = 0;
		int failedCounter = 0;
		Set<String> proccessed = new HashSet<String>();
				
        while (isRunning && !(shouldStop && listsWereEmptyOnce)) {
            try {
            	Map<String, Media> unindexedMedia = new HashMap<String, Media>();
            	Map<String, Media> indexedMedia = new HashMap<String, Media>();
            	
                final List<Image> imageList = imageDAO.getNotVIndexed(STEP);
                final List<Video> videoList = videoDAO.getNotVIndexed(STEP);
                
                LOGGER.info("image list size " + imageList.size());
                LOGGER.info("video list size " + videoList.size());

                if (imageList.isEmpty() && videoList.isEmpty()) {
                    try {
                    	listsWereEmptyOnce = true;
                    	if(shouldStop) {
                    		break;
                    	}
   
                        Thread.sleep(INDEXING_PERIOD);
                    } catch (InterruptedException ie) {

                    }
                } 
                else {
        			// if there are more task to submit and the downloader can accept more tasks then submit
        			while (canAcceptMoreTasks()) {
        				for (Image image : imageList) {
        					if(proccessed.contains(image.getId()))
        						continue;
        					
        					proccessed.add(image.getId());
        					unindexedMedia.put(image.getId(), image);
        					submitTask(image);
        					submittedCounter++;
        				}
        				
        				for(Video video : videoList) {
        					if(proccessed.contains(video.getId()))
        						continue;
        					
        					proccessed.add(video.getId());
        					unindexedMedia.put(video.getId(), video);
        					submitTask(video);
        					submittedCounter++;
        				}
        			}
        			
        			// if are submitted tasks that are pending completion ,try to consume
        			while (completedCounter + failedCounter < submittedCounter) {
        				try {
        					MediaCallableResult result = getResultWait();
        					if(result.vector != null && result.vector.length > 0) {
        						Media media = result.media;
        						if (_indexer.index(media, result.vector)) {
        							indexedMedia.put(media.getId(), media);
        							media.addAnnotation(ld);
        							if(media instanceof Image) {
        								Query<Image> q = imageDAO.createQuery().filter("url", media.getUrl());
        								UpdateOperations<Image> ops = imageDAO.createUpdateOperations().add("annotations", ld);
        								imageDAO.update(q, ops);        							}
        							else if(media instanceof Video) {
        								Query<Video> q = videoDAO.createQuery().filter("url", media.getUrl());
        								UpdateOperations<Video> ops = videoDAO.createUpdateOperations().add("annotations", ld);
        								videoDAO.update(q, ops);
        							}
        							else {
        								LOGGER.info("Unknown instance of " + media.getId());
        							}
        							
                                    if (_publisher != null) {
                                        _publisher.publish(MorphiaManager.getMorphia().toDBObject(media).toString());
                                    }
                                } 
        						else {
        							LOGGER.info("Failed to index" + result.media.getId() + ". Delete media");
                                	deleteMedia(media);
                                }
        					}
        					else {
        						LOGGER.info("Vector for " + result.media.getId() + " is empty. Delete media");
        						deleteMedia(result.media);
        					}
        					completedCounter++;
        					LOGGER.info(completedCounter + " tasks completed!");
        				} catch (Exception e) {
        					failedCounter++;
        					LOGGER.info(failedCounter + " tasks failed!");
        					LOGGER.info(e.getMessage());
        				}
        			}
        			
        			for(String mId : indexedMedia.keySet()) {
        				unindexedMedia.remove(mId);
        			}
        			
        			LOGGER.info(unindexedMedia.size() + " media failed to be indexed!");
        			for(Media failedMedia : unindexedMedia.values()) {
        				try {
        					deleteMedia(failedMedia);
        				}
        				catch(Exception e) {
        					LOGGER.error("Exception during deletion of " + failedMedia.getId(), e);
        				}
        			}	
                }
                
                if (_publisher != null) {
                    _publisher.close();
                }
            } 
            catch (IllegalStateException ex) {
               LOGGER.error("Trying to recreate collections. IllegalStateException: " + ex.getMessage());
                try {
                    imageDAO = new MediaDAO<>(Image.class, collection);
                    videoDAO = new MediaDAO<>(Video.class, collection);
                    pageDAO = new ObjectDAO<>(Webpage.class, collection);
                }
                catch(Exception e) {
                    LOGGER.error("Could not recreate collections. Exception: " + e.getMessage());
                }
            }
            catch(Exception other){
                LOGGER.error("Exception " + other.getMessage());
            }
        }
    }

    public void stop() {
        isRunning = false;
        executor.shutdown(); // Disable new tasks from being submitted
		try {
			// Wait a while for existing tasks to terminate
			if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
				executor.shutdownNow(); // Cancel currently executing tasks
				// Wait a while for tasks to respond to being cancelled
				if (!executor.awaitTermination(60, TimeUnit.SECONDS))
					System.err.println("Pool did not terminate");
			}
		} catch (InterruptedException ie) {
			// (Re-)Cancel if current thread also interrupted
			executor.shutdownNow();
			// Preserve interrupt status
			Thread.currentThread().interrupt();
		}
    }

    public void stopWhenFinished() {
        shouldStop = true;
    }
    
    private void deleteMedia(Media media) {
    	if(media instanceof Image) {
    		System.out.println("Deleting image " + media.getId());
        	imageDAO.delete((Image)media);
        	pageDAO.deleteById(media.getId());
        	if (LinkDetectionRunner.LAST_POSITION > 0)
        		LinkDetectionRunner.LAST_POSITION--;
		}
        else if(media instanceof Video) {
        	System.out.println("Deleting video " + media.getId());
			videoDAO.delete((Video)media);
		}
    	else {
    		System.out.println("Unknown instance for " + media.getId());
    	}
    }
    
    public MediaCallableResult getResultWait() throws Exception {
		try {
			Future<MediaCallableResult> future = pool.take();
			MediaCallableResult imdr = future.get();
			
			return imdr;
		} catch (InterruptedException e) {
			throw e;
		} finally {
			// in any case (Exception or not) the numPendingTask should be reduced
			numPendingTasks--;
		}
	}

	public void submitTask(Media media) {
		Callable<MediaCallableResult> call = new MediaCallable(media, collection);
		pool.submit(call);
		numPendingTasks++;
	}
	
	public boolean canAcceptMoreTasks() {
		if (numPendingTasks < maxNumPendingTasks) {
			return true;
		} 
		else {
			return false;
		}
	}

    public static void main(String[] args) throws Exception {
        Configuration.load("local.properties");
        MorphiaManager.setup("127.0.0.1");
        VisualIndexer.init(true);
        IndexingRunner runner = new IndexingRunner("tessdfasdftest");
        Thread t = new Thread(runner);
        t.start();
    }
}
