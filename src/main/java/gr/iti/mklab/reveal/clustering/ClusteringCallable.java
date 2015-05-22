package gr.iti.mklab.reveal.clustering;

import gr.iti.mklab.reveal.util.Configuration;
import gr.iti.mklab.reveal.visual.VisualIndexer;
import gr.iti.mklab.reveal.visual.VisualIndexerFactory;
import gr.iti.mklab.simmo.core.annotations.Clustered;
import gr.iti.mklab.simmo.core.annotations.lowleveldescriptors.LocalDescriptors;
import gr.iti.mklab.simmo.core.cluster.Cluster;
import gr.iti.mklab.simmo.core.items.Image;
import gr.iti.mklab.simmo.core.items.Media;
import gr.iti.mklab.simmo.core.items.Video;
import gr.iti.mklab.simmo.core.morphia.MediaDAO;
import gr.iti.mklab.simmo.core.morphia.MorphiaManager;
import org.apache.commons.lang.ArrayUtils;
import org.mongodb.morphia.dao.BasicDAO;
import org.mongodb.morphia.dao.DAO;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A clustering callable which clusters the given number of images and videos from
 * the specified collection using the supplied configuration options.
 *
 * @author kandreadou
 */
public class ClusteringCallable implements Callable<List<org.apache.commons.math3.ml.clustering.Cluster<ClusterableMedia>>> {

    private String collection;
    private int count;
    private double eps;
    private int minpoints;

    public ClusteringCallable(String collection, int count, double eps, int minpoints){
        this.collection = collection;
        this.count = count;
        this.eps = eps;
        this.minpoints = minpoints;
    }

    @Override
    public List<org.apache.commons.math3.ml.clustering.Cluster<ClusterableMedia>> call() throws Exception {
        System.out.println("DBSCAN for " + collection + " eps= " + eps + " minpoints= " + minpoints + " count= " + count);
        //First get the existing clusters for this collection
        DAO<Cluster, String> clusterDAO = new BasicDAO<>(gr.iti.mklab.simmo.core.cluster.Cluster.class, MorphiaManager.getMongoClient(), MorphiaManager.getMorphia(), MorphiaManager.getDB(collection).getName());
        List<Cluster> clustersINDB = clusterDAO.find().asList();
        List<org.apache.commons.math3.ml.clustering.Cluster<ClusterableMedia>> existingClusters = new ArrayList<>();
        clustersINDB.stream().forEach(dbCluster -> {
            org.apache.commons.math3.ml.clustering.Cluster<ClusterableMedia> item = new org.apache.commons.math3.ml.clustering.Cluster<>();
            dbCluster.getMembers().stream().forEach(mediaItem -> {
                try {
                    item.addPoint(new ClusterableMedia((Media) mediaItem, ArrayUtils.toPrimitive(
                            VisualIndexerFactory.getVisualIndexer(collection).getVector(((Media) mediaItem).getId()))));
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            });
            existingClusters.add(item);
        });
        List<ClusterableMedia> list = new ArrayList<>();
        //images
        MediaDAO<Image> imageDAO = new MediaDAO<>(Image.class, collection);

        List<Image> images = imageDAO.getIndexedNotClustered(count);
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
        //videos
        MediaDAO<Video> videoDAO = new MediaDAO<>(Video.class, collection);
        List<Video> videos = videoDAO.getIndexedNotClustered(count);
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
        DBSCANClustererIncr<ClusterableMedia> clusterer = new DBSCANClustererIncr(eps, minpoints);
        List<org.apache.commons.math3.ml.clustering.Cluster<ClusterableMedia>> centroids = clusterer.clusterIncremental(list, existingClusters);
        clusterDAO.deleteByQuery(clusterDAO.createQuery());
        System.out.println("DBSCAN NUMBER OF CLUSTERS " + centroids.size());
        for (org.apache.commons.math3.ml.clustering.Cluster<ClusterableMedia> c : centroids) {
            gr.iti.mklab.simmo.core.cluster.Cluster cluster = new gr.iti.mklab.simmo.core.cluster.Cluster();
            cluster.setSize(c.getPoints().size());
            c.getPoints().stream().forEach(clusterable -> {
                cluster.addMember(clusterable.item);
                Media media = clusterable.item;
                media.addAnnotation(new Clustered(cluster.getId()));
                if (media instanceof Image) {
                    imageDAO.save((Image) media);
                } else {
                    videoDAO.save((Video) media);
                }
            });
            clusterDAO.save(cluster);
        }
        return centroids;
    }

    public static void main(String[] args) throws Exception {
        Configuration.load("local.properties");
        MorphiaManager.setup("127.0.0.1");
        VisualIndexer.init();
        ExecutorService clusteringExecutor = Executors.newSingleThreadExecutor();
        clusteringExecutor.submit(new ClusteringCallable("camerona", 500, 0.9, 2)).get();
        clusteringExecutor.shutdown();
        MorphiaManager.tearDown();
    }
}
