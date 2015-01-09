package gr.iti.mklab.reveal.web;

import gr.iti.mklab.reveal.util.MediaItem;

import java.net.MalformedURLException;

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

        public SimilarityResponse(MediaItem item, double distance) throws MalformedURLException {
            this.item = item;
            this.distance = distance;
        }
    }

}
