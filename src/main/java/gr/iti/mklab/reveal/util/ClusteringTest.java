package gr.iti.mklab.reveal.util;

import gr.iti.mklab.reveal.mongo.RevealMediaItemDaoImpl;
import gr.iti.mklab.reveal.visual.IndexingManager;
import gr.iti.mklab.visual.vectorization.ImageVectorization;
import gr.iti.mklab.visual.vectorization.ImageVectorizationResult;
import org.apache.commons.math3.ml.clustering.*;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;


/**
 * Created by kandreadou on 1/21/15.
 */
public class ClusteringTest {

    private final static String CLUSTER_TEST_FOLDER = "/home/kandreadou/Pictures/clustertest/";

    public static void main(String[] args) throws Exception {
        ClusteringTest t = new ClusteringTest();
        t.testDBSCANClusterer();
    }

    private void testDBSCANClusterer() throws Exception {
        IndexingManager.getInstance();
        RevealMediaItemDaoImpl mediaDao = new RevealMediaItemDaoImpl("160.40.51.20", "Showcase", "MediaItems");
        List<ClusterableItem> list = new ArrayList<>();
        List<MediaItem> items = mediaDao.getMediaItems(0, 10000, "image");
        for (MediaItem item : items) {
            BufferedImage img = ImageIO.read(new URL(item.getUrl()));
            ImageVectorization imvec = new ImageVectorization(item.getUrl(), img, 1024, 768 * 512);
            ImageVectorizationResult imvr = imvec.call();
            double[] vector = imvr.getImageVector();
            list.add(new ClusterableItem(item, vector));
        }

        DBSCANClusterer<ClusterableItem> clusterer = new DBSCANClusterer(1.25, 5);
        List<Cluster<ClusterableItem>> centroids = clusterer.cluster(list);
        System.out.println("NUMBER OF CLUSTERS " + centroids.size());
        for (Cluster<ClusterableItem> c : centroids) {
            String dirName = CLUSTER_TEST_FOLDER + "dbscan/cluster" + c.getPoints().size() + "_" + System.currentTimeMillis();
            if (new File(dirName).mkdirs()) {
                System.out.println("CLUSTER size " + c.getPoints().size());
                for (ClusterableItem i : c.getPoints()) {
                    BufferedImage img = ImageIO.read(new URL(i.item.getUrl()));
                    ImageIO.write(img, "jpg", new File(dirName + '/' + i.item.getId()));
                    //System.out.println(i.item.getUrl());
                }
            }
        }

        KMeansPlusPlusClusterer kmeans = new KMeansPlusPlusClusterer(centroids.size());
        List<CentroidCluster<ClusterableItem>> kmeansCentroids = kmeans.cluster(list);
        for (CentroidCluster<ClusterableItem> c : kmeansCentroids) {
            String dirName = CLUSTER_TEST_FOLDER + "kmeans/cluster" + c.getPoints().size() + "_" + System.currentTimeMillis();
            if (new File(dirName).mkdirs()) {
                System.out.println("CLUSTER size " + c.getPoints().size());
                for (ClusterableItem i : c.getPoints()) {
                    BufferedImage img = ImageIO.read(new URL(i.item.getUrl()));
                    ImageIO.write(img, "jpg", new File(dirName + '/' + i.item.getId()));
                    //System.out.println(i.item.getUrl());
                }
            }

        }
    }

    private void testKmeans() throws Exception {
        IndexingManager.getInstance();
        RevealMediaItemDaoImpl mediaDao = new RevealMediaItemDaoImpl("160.40.51.20", "Showcase", "MediaItems");
        List<ClusterableItem> list = new ArrayList<>();
        List<MediaItem> items = mediaDao.getMediaItems(100, 1000, "image");
        for (MediaItem item : items) {
            BufferedImage img = ImageIO.read(new URL(item.getUrl()));
            ImageVectorization imvec = new ImageVectorization(item.getUrl(), img, 1024, 768 * 512);
            ImageVectorizationResult imvr = imvec.call();
            double[] vector = imvr.getImageVector();
            list.add(new ClusterableItem(item, vector));
        }
        KMeansPlusPlusClusterer clusterer = new KMeansPlusPlusClusterer(38);
        List<CentroidCluster<ClusterableItem>> centroids = clusterer.cluster(list);
        for (CentroidCluster<ClusterableItem> c : centroids) {
            System.out.println("CENTROID: " + c.getCenter());
            for (ClusterableItem i : c.getPoints())
                System.out.println(i.item.getUrl());
        }
    }

    class ClusterableItem implements Clusterable {

        private MediaItem item;
        private double[] vector;

        public ClusterableItem(MediaItem item, double[] vector) {
            this.item = item;
            this.vector = vector;
        }

        @Override
        public double[] getPoint() {
            return vector;
        }
    }
}
