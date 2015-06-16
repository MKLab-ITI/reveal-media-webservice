package gr.iti.mklab.reveal.crawler;

import gr.iti.mklab.reveal.rabbitmq.RabbitMQPublisher;
import gr.iti.mklab.reveal.util.Configuration;
import gr.iti.mklab.reveal.visual.VisualIndexer;
import gr.iti.mklab.reveal.visual.VisualIndexerFactory;
import gr.iti.mklab.simmo.core.annotations.lowleveldescriptors.LocalDescriptors;
import gr.iti.mklab.simmo.core.documents.Webpage;
import gr.iti.mklab.simmo.core.items.Image;
import gr.iti.mklab.simmo.core.items.Video;
import gr.iti.mklab.simmo.core.morphia.MediaDAO;
import gr.iti.mklab.simmo.core.morphia.MorphiaManager;
import gr.iti.mklab.simmo.core.morphia.ObjectDAO;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

    public IndexingRunner(String collection) throws ExecutionException {
        System.out.println("Creating IndexingRunner for collection "+collection);
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
        System.out.println("End of constructor ");
    }

    @Override
    public void run() {
        System.out.println("Indexing runner run");
        while (isRunning && !(shouldStop && listsWereEmptyOnce)) {
            final List<Image> imageList = imageDAO.getNotVIndexed(STEP);
            final List<Video> videoList = videoDAO.getNotVIndexed(STEP);
            System.out.println("image list size " + imageList.size());
            System.out.println("video list size " + imageList.size());

            if (imageList.isEmpty() && videoList.isEmpty()) {
                try {
                    listsWereEmptyOnce = true;
                    Thread.sleep(INDEXING_PERIOD);
                } catch (InterruptedException ie) {

                }
            } else {

                for (Image image : imageList) {
                    System.out.println("Checking image "+image.getId());
                    if (_indexer.index(image)) {
                        image.addAnnotation(ld);
                        imageDAO.save(image);
                        if (_publisher != null)
                            _publisher.publish(MorphiaManager.getMorphia().toDBObject(image).toString());
                    } else {
                        System.out.println("Deleting image "+image.getId());
                        imageDAO.delete(image);
                        pageDAO.deleteById(image.getId());
                        if (LinkDetectionRunner.LAST_POSITION > 0)
                            LinkDetectionRunner.LAST_POSITION--;
                    }
                }
                for (Video video : videoList) {
                    if (_indexer.index(video)) {
                        video.addAnnotation(ld);
                        videoDAO.save(video);
                    } else {
                        videoDAO.delete(video);
                        //pageDAO.deleteById(video.getId());
                    }
                }

            }
        }
        if (_publisher != null)
            _publisher.close();
    }

    public void stop() {
        isRunning = false;
    }

    public void stopWhenFinished() {
        shouldStop = true;
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
