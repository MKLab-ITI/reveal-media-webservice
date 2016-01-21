package gr.iti.mklab.reveal.crawler;

import gr.iti.mklab.reveal.rabbitmq.RabbitMQPublisher;
import gr.iti.mklab.reveal.util.Configuration;
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
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * A runnable that indexes all non indexed images found in the specified collection
 * and waits if there are no new images or videos to index
 *
 * @author kandreadou
 */
public class IndexingRunner implements Runnable {

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
        System.out.println("Creating IndexingRunner for collection "+collection);
        this.collection = collection;
        _indexer = VisualIndexerFactory.getVisualIndexer(collection);
        System.out.println("After creating the indexer ");
        if (Configuration.PUBLISH_RABBITMQ)
            _publisher = new RabbitMQPublisher("localhost", collection);
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
        while (isRunning && !(shouldStop && listsWereEmptyOnce)) {
            try {
                final List<Image> imageList = imageDAO.getNotVIndexed(STEP);
                final List<Video> videoList = videoDAO.getNotVIndexed(STEP);
                System.out.println("image list size " + imageList.size());
                System.out.println("video list size " + videoList.size());

                if (imageList.isEmpty() && videoList.isEmpty()) {
                    try {
                        listsWereEmptyOnce = true;
                        Thread.sleep(INDEXING_PERIOD);
                    } catch (InterruptedException ie) {

                    }
                } else {

        			// if there are more task to submit and the downloader can accept more tasks then submit
        			while (canAcceptMoreTasks()) {
        				for (Image image : imageList) {
        					submitTask(image);
        					submittedCounter++;
        				}
        				for(Video video:videoList){
        					submitTask(video);
        					submittedCounter++;
        				}
        			}
        			// if are submitted taks that are pending completion ,try to consume
        			if (completedCounter + failedCounter < submittedCounter) {
        				try {
        					MediaCallableResult result = getResultWait();
        					if(result.vector!=null && result.vector.length>0){
        						Media media = result.media;
        						if (_indexer.index(media, result.vector)) {
        							media.addAnnotation(ld);
        							if(media instanceof Image){
        								imageDAO.save((Image)media);
        							}else{
        								videoDAO.save((Video)media);
        							}
                                    if (_publisher != null)
                                        _publisher.publish(MorphiaManager.getMorphia().toDBObject(media).toString());
                                } else {
                                    System.out.println("Deleting image " + media.getId());
                                    if(media instanceof Image){
                                    	 imageDAO.delete((Image)media);
                                         pageDAO.deleteById(media.getId());
                                         if (LinkDetectionRunner.LAST_POSITION > 0)
                                             LinkDetectionRunner.LAST_POSITION--;
        							}else{
        								videoDAO.delete((Video)media);
        							}
                                }
        					}
        					completedCounter++;
        					System.out.println(completedCounter + " tasks completed!");
        				} catch (Exception e) {
        					failedCounter++;
        					System.out.println(failedCounter + " tasks failed!");
        					System.out.println(e.getMessage());
        				}
        			}
                }
                if (_publisher != null)
                    _publisher.close();
            } catch (IllegalStateException ex) {
                System.out.println("IllegalStateException "+ex);
                System.out.println("Trying to recreate collections");
                try {
                    imageDAO = new MediaDAO<>(Image.class, collection);
                    videoDAO = new MediaDAO<>(Video.class, collection);
                    pageDAO = new ObjectDAO<>(Webpage.class, collection);
                }catch(Exception e){
                    System.out.println("Exception "+e);
                    System.out.println("Could not recreate collections");
                }
            }catch(Exception other){
                System.out.println("Exception "+other);
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
    
    public MediaCallableResult getResultWait() throws Exception {
		try {
			MediaCallableResult imdr = pool.take().get();
			return imdr;
		} catch (Exception e) {
			throw e;
		} finally {
			// in any case (Exception or not) the numPendingTask should be reduced
			numPendingTasks--;
		}
	}

	public void submitTask(Media media) {
		Callable<MediaCallableResult> call = new MediaCallable(media);
		pool.submit(call);
		numPendingTasks++;
	}
	
	public boolean canAcceptMoreTasks() {
		if (numPendingTasks < maxNumPendingTasks) {
			return true;
		} else {
			return false;
		}
	}

    public static void main(String[] args) throws Exception {
        Configuration.load("local.properties");
        MorphiaManager.setup("127.0.0.1");
        VisualIndexer.init();
        IndexingRunner runner = new IndexingRunner("tessdfasdftest");
        Thread t = new Thread(runner);
        t.start();
    }
}
