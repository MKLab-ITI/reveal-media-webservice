package gr.iti.mklab.reveal.solr;

import eu.socialsensor.framework.client.search.Bucket;
import eu.socialsensor.framework.client.search.Facet;
import eu.socialsensor.framework.client.search.SearchEngineResponse;
import eu.socialsensor.framework.client.search.solr.SolrMediaItem;
import eu.socialsensor.framework.client.search.solr.SolrMediaItemHandler;
import eu.socialsensor.framework.client.search.solr.SolrWebPage;
import eu.socialsensor.framework.common.domain.MediaItem;
import eu.socialsensor.framework.common.domain.WebPage;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import java.net.MalformedURLException;
import java.util.*;

/**
 * Created by kandreadou on 9/29/14.
 */
public class SolrManager {

    protected SolrServer server;
    private static SolrManager instance;


    // Private constructor prevents instantiation from other classes
    private SolrManager(String collection) throws Exception {
        server = new HttpSolrServer(collection);
        System.out.println(server.ping());
    }

    // implementing Singleton pattern
    public static SolrManager getInstance(String collection) throws Exception {
        if (instance == null) {
            instance = new SolrManager(collection);
        }
        return instance;
    }

    public static void main(String[] args) throws Exception {


        SolrManager mySolr = SolrManager.getInstance("http://160.40.51.20:8080/solr/WebPages");
        List<WebPage> items = mySolr.test();
        for (WebPage item : items) {
            System.out.println(item.toJSONString());
        }
    }

    public List<WebPage> test() {

        String query = "text:this";
        List<WebPage> mediaItems = collectMediaItemsByQuery("putin", 100);
        return mediaItems;

    }

    public List<WebPage> collectMediaItemsByQuery(String query, int size) {
        if (query.equals("")) {
            return null;
        }
        // TEST CODE FOR MEDIA RETRIEVAL
        query = query.replaceAll("[\"()]", " ");
        query = query.trim();
        // Join query parts with AND
        String[] queryParts = query.split("\\s+");
        query = StringUtils.join(queryParts, " AND ");
        //Retrieve multimedia content that is stored in solr
        if (!query.contains("title") && !query.contains("description")) {
            query = "((title : " + query + ") OR (text:" + query + "))";
            //query = "(title: " + query + " )";
        }

        SolrQuery solrQuery = new SolrQuery(query);
        solrQuery.setRows(size);


        Logger.getRootLogger().info("Solr Query : " + query);
        SearchEngineResponse<WebPage> response = search(solrQuery);
        if (response != null) {
            return response.getResults();
        }
        return null;
    }

    private SearchEngineResponse<WebPage> search(SolrQuery query) {
        SearchEngineResponse<WebPage> response = new SearchEngineResponse<WebPage>();
        QueryResponse rsp;
        try {
            rsp = server.query(query);
        } catch (SolrServerException e) {
            e.printStackTrace();
            Logger.getRootLogger().info(e.getMessage());
            return null;
        }
        List<SolrWebPage> solrWebPages = rsp.getBeans(SolrWebPage.class);
        List<WebPage> webPages = new ArrayList<WebPage>();
        for (SolrWebPage solrWebPage : solrWebPages) {
            try {
                WebPage webPage = solrWebPage.toWebPage();
                webPages.add(webPage);
            } catch (MalformedURLException ex) {
                Logger.getRootLogger().error(ex.getMessage());
            }
        }
        response.setResults(webPages);
        return response;
    }

    public WebPage toWebPage(SolrWebPage page) throws MalformedURLException {

        WebPage webPage = new WebPage(page.getUrl(), page.getReference());

        webPage.setTitle(page.getTitle());
        webPage.setStreamId(page.getStreamId());
        webPage.setDate(page.getDate());


        return webPage;
    }

}
