package gr.iti.mklab.reveal.util;

import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multisets;
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

    public static Multiset<Integer> DBSCAN_CLUSTERS = ConcurrentHashMultiset.create();
    public static Multiset<Integer> KMEANS_CLUSTERS = ConcurrentHashMultiset.create();

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
            try {
                BufferedImage img = ImageIO.read(new URL(item.getUrl()));
                ImageVectorization imvec = new ImageVectorization(item.getUrl(), img, 1024, 768 * 512);
                ImageVectorizationResult imvr = imvec.call();
                double[] vector = imvr.getImageVector();
                list.add(new ClusterableItem(item, vector));
            } catch (Exception ex) {
            }
        }

        DBSCANClusterer<ClusterableItem> clusterer = new DBSCANClusterer(1.2, 5);
        List<Cluster<ClusterableItem>> centroids = clusterer.cluster(list);
        System.out.println("DBSCAN NUMBER OF CLUSTERS " + centroids.size());
        for (Cluster<ClusterableItem> c : centroids) {
            String dirName = CLUSTER_TEST_FOLDER + "dbscan/cluster" + c.getPoints().size() + "_" + System.currentTimeMillis();
            if (new File(dirName).mkdirs()) {
                DBSCAN_CLUSTERS.add(c.getPoints().size());
                //System.out.println(c.getPoints().size());
                for (ClusterableItem i : c.getPoints()) {
                    String itemUrl = i.item.getUrl();
                    String suffix = itemUrl.substring(itemUrl.lastIndexOf('.') + 1, itemUrl.length());
                    BufferedImage img = ImageIO.read(new URL(itemUrl));
                    ImageIO.write(img, suffix, new File(dirName + '/' + i.item.getId() + '.' + suffix));
                    //System.out.println(i.item.getUrl());
                }
            }
        }

        KMeansPlusPlusClusterer kmeans = new KMeansPlusPlusClusterer(centroids.size());
        List<CentroidCluster<ClusterableItem>> kmeansCentroids = kmeans.cluster(list);

        for (CentroidCluster<ClusterableItem> c : kmeansCentroids) {
            String dirName = CLUSTER_TEST_FOLDER + "kmeans/cluster" + c.getPoints().size() + "_" + System.currentTimeMillis();
            if (new File(dirName).mkdirs()) {
                KMEANS_CLUSTERS.add(c.getPoints().size());
                //System.out.println(c.getPoints().size());
                for (ClusterableItem i : c.getPoints()) {
                    String itemUrl = i.item.getUrl();
                    String suffix = itemUrl.substring(itemUrl.lastIndexOf('.') + 1, itemUrl.length());
                    BufferedImage img = ImageIO.read(new URL(itemUrl));
                    ImageIO.write(img, suffix, new File(dirName + '/' + i.item.getId() + '.' + suffix));
                    //System.out.println(i.item.getUrl());
                }
            }

        }
        System.out.println("DBSCAN element count");
        Iterable<Multiset.Entry<Integer>> cases =
                Multisets.copyHighestCountFirst(DBSCAN_CLUSTERS).entrySet();
        for (Multiset.Entry<Integer> s : cases) {
            System.out.println(s.getElement() + " " + s.getCount());
        }
        System.out.println("KMEANS element count");
        cases =
                Multisets.copyHighestCountFirst(KMEANS_CLUSTERS).entrySet();
        for (Multiset.Entry<Integer> s : cases) {
            System.out.println(s.getElement() + " " + s.getCount());
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
