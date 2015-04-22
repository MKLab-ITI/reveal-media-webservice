package gr.iti.mklab.reveal.crawler;

import gr.iti.mklab.reveal.configuration.Configuration;
import gr.iti.mklab.reveal.visual.VisualIndexer;
import gr.iti.mklab.reveal.visual.VisualIndexerFactory;
import gr.iti.mklab.simmo.core.annotations.lowleveldescriptors.LocalDescriptors;
import gr.iti.mklab.simmo.core.items.Image;
import gr.iti.mklab.simmo.core.items.Video;
import gr.iti.mklab.simmo.core.morphia.MediaDAO;
import gr.iti.mklab.simmo.core.morphia.MorphiaManager;

import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Created by kandreadou on 4/20/15.
 */
public class IndexingRunner implements Runnable {

    private final static int STEP = 100;
    private VisualIndexer _indexer;
    private MediaDAO<Image> imageDAO;
    private MediaDAO<Video> videoDAO;
    private LocalDescriptors ld;
    private boolean isRunning = true;

    public IndexingRunner(String collection) throws ExecutionException {
        _indexer = VisualIndexerFactory.getVisualIndexer(collection);
        imageDAO = new MediaDAO<>(Image.class, collection);
        videoDAO = new MediaDAO<>(Video.class, collection);
        ld = new LocalDescriptors();
        ld.setDescriptorType(LocalDescriptors.DESCRIPTOR_TYPE.SURF);
        ld.setFeatureEncoding(LocalDescriptors.FEATURE_ENCODING.Vlad);
        ld.setNumberOfFeatures(1024);
        ld.setFeatureEncodingLibrary("multimedia-indexing");
    }

    @Override
    public void run() {
        List<Image> imageList;
        List<Video> videoList;
        while (isRunning) {
            imageList = imageDAO.getNotVIndexed(STEP);
            videoList = videoDAO.getNotVIndexed(STEP);

            if (imageList.isEmpty() && videoList.isEmpty()) {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ie) {

                }
            } else {
                for (Image image : imageList) {
                    if (_indexer.index(image)) {
                        image.addAnnotation(ld);
                        imageDAO.save(image);
                    } else {
                        imageDAO.delete(image);
                    }
                }
                for (Video video : videoList) {
                    if (_indexer.index(video)) {
                        video.addAnnotation(ld);
                        videoDAO.save(video);
                    } else {
                        videoDAO.delete(video);
                    }
                }
            }
        }
    }

    public void stop() {
        isRunning = false;
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
