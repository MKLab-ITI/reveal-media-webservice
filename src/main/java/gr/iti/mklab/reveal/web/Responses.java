package gr.iti.mklab.reveal.web;

import gr.iti.mklab.simmo.items.Image;
import gr.iti.mklab.simmo.items.Video;
import gr.iti.mklab.simmo.jobs.CrawlJob;

import java.net.MalformedURLException;
import java.util.*;

/**
 * Created by kandreadou on 1/9/15.
 */
public class Responses {

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

        protected Image image;

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
            if (image != null ? !image.getId().equals(that.image.getId()) : that.image != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result;
            long temp;
            temp = Double.doubleToLongBits(distance);
            result = (int) (temp ^ (temp >>> 32));
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

    public static class CrawlStatus extends CrawlJob {

        public CrawlStatus(){

        }

        public CrawlStatus(CrawlJob req){
            this.keywords = req.getKeywords();
            this.requestState = req.getState();
            this.collection = req.getCollection();
            this.crawlDataPath = req.getCrawlDataPath();
            this.creationDate = req.getCreationDate();
            this.lastStateChange = req.getLastStateChange();
            this.id = req.getId();
            this.isNew = req.isNew();
        }

        public long numImages;

        public long numVideos;

        public Image image;

        public Video video;

        public long duration;

        public String lastItemInserted;
    }

    public static void main(String[] args) throws Exception {
        Image im = new Image();
        //im.setId(new ObjectId("54b8f9d2e4b0daec5c5e5921").toString());
        SimilarityResponse res = new SimilarityResponse(im, 1.2);
        Image im2 = new Image();
        //im2.setId(new ObjectId("54b8f9d2e4b0daec5c5e5921").toString());
        SimilarityResponse res2 = new SimilarityResponse(im, 1.2);

        List<SimilarityResponse> set = new ArrayList<>();
        set.add(res);
        boolean contains = set.contains(res2);

        int m = 0;
    }

}
