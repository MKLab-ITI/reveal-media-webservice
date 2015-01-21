package gr.iti.mklab.reveal.solr;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;

import java.util.List;

/**
 * Created by kandreadou on 1/20/15.
 */
public class SimmoSolrManager {

    private final HttpSolrServer server;

    public SimmoSolrManager(String collectionUrl) throws Exception {
        server = new HttpSolrServer(collectionUrl);
        server.ping();
    }

    public static void main(String[] args) throws Exception {
        String collectionURL = "http://160.40.51.20:8080/solr/Images";
        SimmoSolrManager mgr = new SimmoSolrManager(collectionURL);
        mgr.searchImagesByQuery("canada", null, 10);
        /*MorphiaManager.setup("deutschland","160.40.51.20");
        MediaDAO<Image> dao = new MediaDAO<>(Image.class);
        List<Image> list = dao.getItems(50,0);
        for(Image i:list){
            SolrImage temp = new SolrImage(i, "germany");
            boolean inserted = mgr.insert(temp);
            System.out.println("inserted "+inserted);
        }
        MorphiaManager.tearDown();*/
    }

    public boolean insert(SolrImage image) throws Exception {
        UpdateResponse response = server.addBean(image);
        return response.getStatus() == 0;
    }

    public List<SolrImage> searchImagesByQuery(String query, String collection, int size) {
        String solrStr = "q=" + query;
        if (collection != null)
            solrStr += " AND collection:" + collection;
        SolrQuery solrQuery = new SolrQuery(solrStr);
        solrQuery.setRows(size);

        Logger.getRootLogger().info("Solr Query : " + query);
        QueryResponse rsp;
        try {
            rsp = server.query(solrQuery);
            if (rsp != null) {
                List<SolrImage> solrImages = rsp.getBeans(SolrImage.class);
                return solrImages;
            }
        } catch (SolrServerException e) {
            e.printStackTrace();
            Logger.getRootLogger().info(e.getMessage());
            return null;
        }
        return null;
    }

}
