package gr.iti.mklab.reveal.visual;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class JsonResultSet {
    private static DecimalFormat df = new DecimalFormat("#.###");
    @Expose
    @SerializedName(value = "results")
    List<JsonResult> results = new ArrayList<JsonResult>();

    public void addResult(String id, int rank, double distance) {
        JsonResult result = new JsonResult(id, rank, distance);
        results.add(result);
    }

    public List<JsonResult> getResults() {
        return results;
    }

    public class JsonResult {
    	
        @Expose
        @SerializedName(value = "id")
        private String id;
        
        @Expose
        @SerializedName(value = "rank")
        private int rank;
        
        @Expose
        @SerializedName(value = "score")
        private String score;

        public JsonResult(String id, int rank, double distance) {
            this.id = id;
            this.rank = rank;
            // transform distance into similarity
            double similarity = (2.0 - Math.sqrt(distance)) / 2.0;
            // format the score
            this.score = df.format(similarity);
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public int getRank() {
            return rank;
        }

        public void setRank(int rank) {
            this.rank = rank;
        }

        public String getScore() {
            return score;
        }

        public void setScore(String score) {
            this.score = score;
        }
    }

    public String toJSON() {
        Gson gson = new GsonBuilder()
                .excludeFieldsWithoutExposeAnnotation()
                .create();
        return gson.toJson(this);
    }

    public static void main(String... args) {
        JsonResultSet s = new JsonResultSet();
        s.addResult("X", 1, 0.2);
        s.addResult("Y", 2, 0.4);
        s.addResult("Z", 3, 0.8);
        System.out.println(s.toJSON());
    }
}
