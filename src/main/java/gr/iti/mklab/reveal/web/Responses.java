package gr.iti.mklab.reveal.web;

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

}
