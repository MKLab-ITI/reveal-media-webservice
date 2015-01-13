package gr.iti.mklab.reveal.crawler;

import com.google.gson.Gson;
;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by kandreadou on 1/12/15.
 */
public class SeedURLGenerator {

    private final static int RESULT_MAX_SIZE = 8;

    public static void main(String[] args) throws Exception {
        createSeedFileForKeywords("charlie hebdo", "paris");
    }

    public static void createSeedFileForKeywords(String... keywords) throws Exception {
        String path = "/home/kandreadou/Documents/" + "seeds" + System.currentTimeMillis() + ".txt";
        PrintWriter writer = new PrintWriter(path, "UTF-8");
        String term = "";
        for (String k : keywords) {
            term += k + "+";
        }
        term = term.substring(0, term.length() - 1);
        Set<String> results = GetGoogleResults(term);
        System.out.println("After GetGoogleResults");
        for (String r : results)
            writer.println(r);
        writer.close();
    }

    private static Set<String> GetGoogleResults(String term) throws Exception {
        int resultsSize = 0;
        int requests =0;
        Set<String> r = new HashSet<>();
        String google;
        String charset = "UTF-8";
        URL url;
        GoogleResult results;
        long start = System.currentTimeMillis();

        while ((resultsSize ==0 || resultsSize != r.size())&&System.currentTimeMillis()-start<10000) {
            System.out.println( " results size " + resultsSize + "r.size " + r.size());
            resultsSize = r.size();
            google = "https://ajax.googleapis.com/ajax/services/search/web?v=1.0&start=" + requests*RESULT_MAX_SIZE + "&rsz="+RESULT_MAX_SIZE+"&q=" + term;
            url = new URL(google + URLEncoder.encode(term, charset));
            URLConnection connection = url.openConnection();
            connection.addRequestProperty("Referer", "www.iti.gr");

            Reader reader = new InputStreamReader(connection.getInputStream(), charset);
            results = new Gson().fromJson(reader, GoogleResult.class);

            if (results.getResponseData() != null) {
                for (int i = 0; i < results.getResponseData().getResults().size(); i++) {
                    String resultURL = URLDecoder.decode(results.getResponseData().getResults().get(i).getUrl(), "UTF-8");
                    r.add(resultURL);
                    System.out.println("Title: " + results.getResponseData().getResults().get(i).getTitle());
                    System.out.println("Url: " + resultURL);
                    System.out.println("Content: " + results.getResponseData().getResults().get(i).getContent());
                    System.out.println();
                }
            }
            requests++;

        }
        return r;
    }

    class GoogleResult {
        private ResponseData responseData;

        public ResponseData getResponseData() {
            return responseData;
        }

        public void setResponseData(ResponseData responseData) {
            this.responseData = responseData;
        }

        class ResponseData {
            private List<Result> results;

            public List<Result> getResults() {
                return results;
            }

            public void setResults(List<Result> results) {
                this.results = results;
            }
        }

        class Result {
            private String url;
            private String titleNoFormatting;
            private String content;

            public String getUrl() {
                return url;
            }

            public String getTitle() {
                return titleNoFormatting;
            }

            public String getContent() {
                return content;
            }

            public void setUrl(String url) {
                this.url = url;
            }

            public void setTitle(String title) {
                this.titleNoFormatting = title;
            }

            public void setContent(String content) {
                this.content = content;
            }
        }
    }

}
