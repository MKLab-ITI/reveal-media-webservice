package gr.iti.mklab.reveal.web;

import gr.iti.mklab.reveal.crawler.CrawlRequest;
import gr.iti.mklab.reveal.util.MediaItem;
import gr.iti.mklab.simmo.items.Image;
import gr.iti.mklab.simmo.items.Video;
import org.bson.types.ObjectId;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by kandreadou on 1/9/15.
 */
public class Responses {

    public static class StatsResponse {

        private String collectionName;

        private int collectionSize;

        public StatsResponse(String name, int size) {
            this.collectionName = name;
            this.collectionSize = size;
        }
    }

    public static class IndexResponse {

        protected boolean success = false;

        protected String message;

        public IndexResponse(boolean success, String message) {
            this.success = success;
            this.message = message;
        }

        public IndexResponse() {
            this.success = true;
        }
    }

    public static class SimilarityResponse {

        protected double distance;

        protected MediaItem item;

        protected Image image;

        public SimilarityResponse(MediaItem item, double distance) throws MalformedURLException {
            this.item = item;
            this.distance = distance;
        }

        public SimilarityResponse(Image image, double distance) throws MalformedURLException {
            this.image = image;
            this.distance = distance;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            SimilarityResponse that = (SimilarityResponse) o;

            if (Double.compare(that.distance, distance) != 0) return false;
            if (image != null ? !image.getObjectId().equals(that.image.getObjectId()) : that.image != null) return false;
            if (item != null ? !item.getId().equals(that.item.getId()) : that.item != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result;
            long temp;
            temp = Double.doubleToLongBits(distance);
            result = (int) (temp ^ (temp >>> 32));
            result = 31 * result + (item != null ? item.hashCode() : 0);
            result = 31 * result + (image != null ? image.hashCode() : 0);
            return result;
        }
    }

    public static class MediaResponse {
        protected List<Image> images = new ArrayList<>();

        protected List<Video> videos = new ArrayList<>();

        protected long numImages;

        protected long numVideos;

        protected long offset;
    }

    public static class CrawlStatus extends CrawlRequest {

        public CrawlStatus(CrawlRequest req){
            this.keywords = req.keywords;
            this.requestState = req.requestState;
            this.collectionName = req.collectionName;
            this.crawlDataPath = req.crawlDataPath;
            this.creationDate = req.creationDate;
            this.lastStateChange = req.lastStateChange;
            this.id = req.id;
            this.isNew = req.isNew;
        }

        public long numImages;

        public long numVideos;

        public Image image;

        public Video video;
    }

    public static void main(String[] args) throws Exception {
        Image im = new Image();
        im.setObjectId(new ObjectId("54b8f9d2e4b0daec5c5e5921"));
        SimilarityResponse res = new SimilarityResponse(im, 1.2);
        Image im2 = new Image();
        im2.setObjectId(new ObjectId("54b8f9d2e4b0daec5c5e5921"));
        SimilarityResponse res2 = new SimilarityResponse(im, 1.2);

        List<SimilarityResponse> set = new ArrayList<>();
        set.add(res);
        boolean contains = set.contains(res2);

        int m = 0;
    }

}
