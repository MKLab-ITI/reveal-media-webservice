package gr.iti.mklab.reveal.visual;

import gr.iti.mklab.reveal.util.Configuration;
import gr.iti.mklab.simmo.core.items.Image;
import gr.iti.mklab.simmo.core.items.Media;
import gr.iti.mklab.simmo.core.morphia.MorphiaManager;
import gr.iti.mklab.visual.aggregation.AbstractFeatureAggregator;
import gr.iti.mklab.visual.aggregation.VladAggregatorMultipleVocabularies;
import gr.iti.mklab.visual.dimreduction.PCA;
import gr.iti.mklab.visual.extraction.AbstractFeatureExtractor;
import gr.iti.mklab.visual.extraction.SURFExtractor;
import gr.iti.mklab.visual.utilities.Normalization;
import gr.iti.mklab.visual.vectorization.ImageVectorization;
import gr.iti.mklab.visual.vectorization.ImageVectorizationResult;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;


/**
 * Created by kandreadou on 2/10/15.
 */
public class VisualIndexer {

    protected static int maxNumPixels = 768 * 512;
    protected static int targetLengthMax = 1024;
    private static PCA pca;
    private static CloseableHttpClient _httpclient;
    private static RequestConfig _requestConfig;
    private static Logger _logger = LoggerFactory.getLogger(VisualIndexer.class);

    private VisualIndexHandler handler;
    private String collection;
    
    public static void init(boolean loadVectorizer) throws Exception {

        _requestConfig = RequestConfig.custom()
                .setSocketTimeout(30000)
                .setConnectTimeout(30000)
                .build();
        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
        _httpclient = HttpClients.custom()
                .setConnectionManager(cm)
                .build();
        
        if(loadVectorizer) {
        	int[] numCentroids = {128, 128, 128, 128};
        	int initialLength = numCentroids.length * numCentroids[0] * AbstractFeatureExtractor.SURFLength;

        	String[] codebookFiles = {
                Configuration.LEARNING_FOLDER + "surf_l2_128c_0.csv",
                Configuration.LEARNING_FOLDER + "surf_l2_128c_1.csv",
                Configuration.LEARNING_FOLDER + "surf_l2_128c_2.csv",
                Configuration.LEARNING_FOLDER + "surf_l2_128c_3.csv"
        	};

        	String pcaFile = Configuration.LEARNING_FOLDER + "pca_surf_4x128_32768to1024.txt";

        	SURFExtractor extractor = new SURFExtractor();
        	ImageVectorization.setFeatureExtractor(extractor);
        	double[][][] codebooks = AbstractFeatureAggregator.readQuantizers(codebookFiles, numCentroids,
        	AbstractFeatureExtractor.SURFLength);
        	
        	ImageVectorization.setVladAggregator(new VladAggregatorMultipleVocabularies(codebooks));
        	if (targetLengthMax < initialLength) {
            	System.out.println("targetLengthMax : " + targetLengthMax + " initialLengh " + initialLength);
            	pca = new PCA(targetLengthMax, 1, initialLength, true);
            	pca.loadPCAFromFile(pcaFile);
            	ImageVectorization.setPcaProjector(pca);
        	}
        }
    }

    public static void init() throws Exception {
    	init(true);
    }

    public VisualIndexer(String collectionName) throws IOException {
        this.collection = collectionName;
        createCollection();
        handler = new VisualIndexHandler("http://" + Configuration.INDEX_SERVICE_HOST + ":8080/VisualIndexService", collectionName);
    }

    public Double[] getVector(String id) {
        return handler.getVector(id);
    }

    /**
     * Only return false if the image has to be deleted, if it is too small,
     * not in cases of error
     *
     * @param media
     * @return
     */
    public boolean index(Media media, double[] vector) {
        if (handler == null)
            throw new IllegalStateException("There is no index for the collection " + collection);
        try{
        	handler.index(media.getId(), vector);
        	return true;
        }catch(Exception e){
        	_logger.error(e.getMessage(), e);
        	return false;
        }
    }

    public boolean createCollection() throws IOException {
        String request = "http://" + Configuration.INDEX_SERVICE_HOST + ":8080/VisualIndexService/rest/visual/add/" + collection;
        HttpGet httpget = new HttpGet(request.replaceAll(" ", "%20"));
        httpget.setConfig(_requestConfig);
        HttpResponse response = _httpclient.execute(httpget);
        StatusLine status = response.getStatusLine();
        int code = status.getStatusCode();
        if (code < 200 || code >= 300) {
            _logger.error("Failed create collection with name " + collection +
                    ". Http code: " + code + " Error: " + status.getReasonPhrase());
            return false;
        }
        HttpEntity entity = response.getEntity();
        if (entity == null) {
            _logger.error("Entity is null for create collection " + collection +
                    ". Http code: " + code + " Error: " + status.getReasonPhrase());
            return false;
        }
        return true;
    }

    public boolean deleteCollection() throws Exception {
        String request = "http://" + Configuration.INDEX_SERVICE_HOST + ":8080/VisualIndexService/rest/visual/delete/" + collection;
        HttpGet httpget = new HttpGet(request.replaceAll(" ", "%20"));
        httpget.setConfig(_requestConfig);
        HttpResponse response = _httpclient.execute(httpget);
        StatusLine status = response.getStatusLine();
        int code = status.getStatusCode();
        if (code < 200 || code >= 300) {
            _logger.error("Failed delete collection with name " + collection +
                    ". Http code: " + code + " Error: " + status.getReasonPhrase());
            return false;
        }
        HttpEntity entity = response.getEntity();
        if (entity == null) {
            _logger.error("Entity is null for create collection " + collection +
                    ". Http code: " + code + " Error: " + status.getReasonPhrase());
            return false;
        }
        return true;
    }

    public static boolean deleteCollection(String thisCollection) throws Exception {
        String request = "http://" + Configuration.INDEX_SERVICE_HOST + ":8080/VisualIndexService/rest/visual/delete/" + thisCollection;
        HttpGet httpget = new HttpGet(request.replaceAll(" ", "%20"));
        httpget.setConfig(_requestConfig);
        HttpResponse response = _httpclient.execute(httpget);
        StatusLine status = response.getStatusLine();
        int code = status.getStatusCode();
        if (code < 200 || code >= 300) {
            _logger.error("Failed delete collection with name " + thisCollection +
                    ". Http code: " + code + " Error: " + status.getReasonPhrase());
            return false;
        }
        HttpEntity entity = response.getEntity();
        if (entity == null) {
            _logger.error("Entity is null for create collection " + thisCollection +
                    ". Http code: " + code + " Error: " + status.getReasonPhrase());
            return false;
        }
        return true;
    }
    
    public int numItems() throws Exception {
        String request = "http://" + Configuration.INDEX_SERVICE_HOST + ":8080/VisualIndexService/rest/visual/statistics/" + collection;
        HttpGet httpget = new HttpGet(request.replaceAll(" ", "%20"));
        httpget.setConfig(_requestConfig);
        HttpResponse response = _httpclient.execute(httpget);
        StatusLine status = response.getStatusLine();
        int code = status.getStatusCode();
        if (code < 200 || code >= 300) {
            _logger.error("Failed delete collection with name " + collection +
                    ". Http code: " + code + " Error: " + status.getReasonPhrase());
            return 0;
        }
        HttpEntity entity = response.getEntity();
        if (entity == null) {
            _logger.error("Entity is null for create collection " + collection +
                    ". Http code: " + code + " Error: " + status.getReasonPhrase());
            return 0;
        }
        String entityAsString = EntityUtils.toString(entity);
        JSONObject jsonObject = new JSONObject(entityAsString);
        return jsonObject.getInt("ivfpqIndex");
    }

    public List<JsonResultSet.JsonResult> findSimilar(String url, double threshold) {
        List<JsonResultSet.JsonResult> results = new ArrayList<>();
        if (handler == null)
            throw new IllegalStateException("There is no index for the collection " + collection);
        HttpGet httpget = null;
        try {
            httpget = new HttpGet(url.replaceAll(" ", "%20"));
            httpget.setConfig(_requestConfig);
            HttpResponse response = _httpclient.execute(httpget);
            StatusLine status = response.getStatusLine();
            int code = status.getStatusCode();
            if (code < 200 || code >= 300) {
                _logger.error("Failed fetch media item " + url + ". URL=" + url +
                        ". Http code: " + code + " Error: " + status.getReasonPhrase());
                throw new IllegalStateException("Failed fetch media item " + url + ". URL=" + url +
                        ". Http code: " + code + " Error: " + status.getReasonPhrase());
            }
            HttpEntity entity = response.getEntity();
            if (entity == null) {
                _logger.error("Entity is null for " + url + ". URL=" + url +
                        ". Http code: " + code + " Error: " + status.getReasonPhrase());
                throw new IllegalStateException("Entity is null for " + url + ". URL=" + url +
                        ". Http code: " + code + " Error: " + status.getReasonPhrase());
            }
            InputStream input = entity.getContent();
            byte[] imageContent = IOUtils.toByteArray(input);
            BufferedImage image = ImageIO.read(new ByteArrayInputStream(imageContent));
            if (image != null) {
                ImageVectorization imvec = new ImageVectorization(url, image, targetLengthMax, maxNumPixels);
                ImageVectorizationResult imvr = imvec.call();
                double[] vector = imvr.getImageVector();
                if (vector == null || vector.length == 0) {
                    _logger.error("Error in feature extraction for " + url);
                    throw new IllegalStateException("Error in feature extraction for " + url);
                }
                results = handler.getSimilarImages(vector, threshold).getResults();

            }
        } catch (Exception e) {
            _logger.error(e.getMessage(), e);

        } finally {
            if (httpget != null) {
                httpget.abort();
            }
        }
        
        return results;
    }

    public static void main(String[] args) throws Exception {
    	
    	Configuration.INDEX_SERVICE_HOST = "160.40.51.20";
    	VisualIndexer.init(false);
        //VisualIndexer v = new VisualIndexer("test_collection");
        VisualIndexer v = VisualIndexerFactory.getVisualIndexer("test_collection");
    	
    	Media media = new Image();
    	media.setId("2");
    	
    	Random r = new Random();
		double[] vector = new double[1024];
		for(int i=0; i<1024; i++) {
			vector[i] = r.nextDouble();
		}
		vector = Normalization.normalizeL2(vector);
		
		v.index(media, vector);
		
		System.out.println(v.numItems());
		
		v.deleteCollection();
    }
}
