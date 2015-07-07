package gr.iti.mklab.reveal.clustering;

import com.aliasi.tokenizer.TokenizerFactory;
import gr.iti.mklab.reveal.util.Configuration;
import gr.iti.mklab.reveal.visual.VisualIndexer;
import gr.iti.mklab.reveal.visual.VisualIndexerFactory;
import gr.iti.mklab.simmo.core.annotations.Clustered;
import gr.iti.mklab.simmo.core.cluster.Clusterable;
import gr.iti.mklab.simmo.core.items.Image;
import gr.iti.mklab.simmo.core.items.Media;
import gr.iti.mklab.simmo.core.items.Video;
import gr.iti.mklab.simmo.core.morphia.MediaDAO;
import gr.iti.mklab.simmo.core.morphia.MorphiaManager;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.math3.ml.clustering.Cluster;
import org.mongodb.morphia.dao.BasicDAO;
import org.mongodb.morphia.dao.DAO;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A clustering callable which incrementally clusters all items in the given collection
 *
 * @author kandreadou
 */
public class ClusterEverythingCallable implements Callable<List<Cluster<ClusterableMedia>>> {

    private String collection;
    private double eps;
    private int minpoints;
    private final static int ITEMS_PER_ITERATION = 2000;

    public ClusterEverythingCallable(String collection, double eps, int minpoints) {
        System.out.println("DBSCAN for " + collection + " eps= " + eps + " minpoints= " + minpoints);
        this.collection = collection;
        this.eps = eps;
        this.minpoints = minpoints;
    }

    @Override
    public List<org.apache.commons.math3.ml.clustering.Cluster<ClusterableMedia>> call() throws Exception {
        System.out.println("Cluster everything callable call method");

        TokenizerFactory tokFactory = new NormalizedTokenizerFactory();
        DBSCANClustererIncr<ClusterableMedia> clusterer = new DBSCANClustererIncr(eps, minpoints);
        DAO<gr.iti.mklab.simmo.core.cluster.Cluster, String> clusterDAO = new BasicDAO<>(gr.iti.mklab.simmo.core.cluster.Cluster.class, MorphiaManager.getMongoClient(), MorphiaManager.getMorphia(), MorphiaManager.getDB(collection).getName());
        List<org.apache.commons.math3.ml.clustering.Cluster<ClusterableMedia>> centroids = null;

        MediaDAO<Image> imageDAO = new MediaDAO<>(Image.class, collection);
        MediaDAO<Video> videoDAO = new MediaDAO<>(Video.class, collection);

        //images
        for (int k = 0; k < imageDAO.count(); k += ITEMS_PER_ITERATION) {
            System.out.println("Clustering images iteration " + k);
            List<ClusterableMedia> list = new ArrayList<>();
            List<Image> images = imageDAO.getItems(ITEMS_PER_ITERATION, k);
            images.stream().forEach(i -> {
                Double[] vector = new Double[0];
                try {
                    vector = VisualIndexerFactory.getVisualIndexer(collection).getVector(i.getId());
                } catch (ExecutionException e) {
                    //ignore
                }
                if (vector != null && vector.length == 1024)
                    list.add(new ClusterableMedia(i, ArrayUtils.toPrimitive(vector)));
            });
            centroids = clusterer.clusterIncremental(list, centroids);
        }
        //videos
        for (int k = 0; k < videoDAO.count(); k += ITEMS_PER_ITERATION) {
            System.out.println("Clustering videos iteration " + k);
            List<ClusterableMedia> list = new ArrayList<>();
            List<Video> videos = videoDAO.getItems(ITEMS_PER_ITERATION, k);
            videos.stream().forEach(i -> {
                Double[] vector = new Double[0];
                try {
                    vector = VisualIndexerFactory.getVisualIndexer(collection).getVector(i.getId());
                } catch (ExecutionException e) {
                    //ignore
                }
                if (vector != null && vector.length == 1024)
                    list.add(new ClusterableMedia(i, ArrayUtils.toPrimitive(vector)));
            });

            centroids = clusterer.clusterIncremental(list, centroids);
        }

        clusterDAO.deleteByQuery(clusterDAO.createQuery());
        System.out.println("DBSCAN NUMBER OF CLUSTERS " + centroids.size());
        for (org.apache.commons.math3.ml.clustering.Cluster<ClusterableMedia> c : centroids) {
            List<Media> initial = new ArrayList<>();
            gr.iti.mklab.simmo.core.cluster.Cluster cluster = new gr.iti.mklab.simmo.core.cluster.Cluster();
            cluster.setSize(c.getPoints().size());
            c.getPoints().stream().forEach(clusterable -> {
                //cluster.addMember(clusterable.item);
                Media media = clusterable.item;
                initial.add(media);
                media.addAnnotation(new Clustered(cluster.getId()));
                if (media instanceof Image) {
                    imageDAO.save((Image) media);
                } else {
                    videoDAO.save((Video) media);
                }
            });
            System.out.println("Initial size " + initial.size());
            List<Media> filteredNomralized = TextDeduplication.filterNormalizedDuplicates(initial, tokFactory);
            System.out.println("After normalization size " + filteredNomralized.size());
            List<Media> filteredJaccard = TextDeduplication.filterMediaJaccard(filteredNomralized, tokFactory, 0.5);
            System.out.println("After jaccard size " + filteredJaccard.size());
            filteredJaccard.stream().forEach(m -> cluster.addMember(m));
            cluster.setSize(filteredJaccard.size());
            if (cluster.getSize() < 100)
                clusterDAO.save(cluster);
        }
        return centroids;
    }

    public static void main(String[] args) throws Exception {
        Configuration.load("local.properties");
        MorphiaManager.setup("160.40.51.20");
        VisualIndexer.init();
        VisualIndexer vi = VisualIndexerFactory.getVisualIndexer("eurogroup");
        ExecutorService clusteringExecutor = Executors.newSingleThreadExecutor();
        clusteringExecutor.submit(new ClusterEverythingCallable("eurogroup", 1.2, 2)).get();
        clusteringExecutor.shutdown();
        MorphiaManager.tearDown();
    }
}
