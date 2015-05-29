package gr.iti.mklab.reveal.visual;

import gr.iti.mklab.reveal.util.Configuration;
import gr.iti.mklab.reveal.util.ImageUtils;
import gr.iti.mklab.simmo.core.items.Image;
import gr.iti.mklab.simmo.core.items.Media;
import gr.iti.mklab.simmo.core.items.Video;
import gr.iti.mklab.simmo.core.morphia.MorphiaManager;
import gr.iti.mklab.visual.aggregation.AbstractFeatureAggregator;
import gr.iti.mklab.visual.aggregation.VladAggregatorMultipleVocabularies;
import gr.iti.mklab.visual.dimreduction.PCA;
import gr.iti.mklab.visual.extraction.AbstractFeatureExtractor;
import gr.iti.mklab.visual.extraction.SURFExtractor;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;


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

    public static void init() throws Exception {

        _requestConfig = RequestConfig.custom()
                .setSocketTimeout(60000)
                .setConnectTimeout(60000)
                .build();
        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
        _httpclient = HttpClients.custom()
                .setConnectionManager(cm)
                .build();
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

    private VisualIndexHandler handler;
    private String collection;

    public VisualIndexer(String collectionName) throws Exception {
        this.collection = collectionName;
        createCollection(collectionName);
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
    public boolean index(Media media) {
        boolean indexed = false;
        if (handler == null)
            throw new IllegalStateException("There is no index for the collection " + collection);
        HttpGet httpget = null;
        try {
            String id = media.getId();
            String url = null;
            if (media instanceof Image)
                url = media.getUrl();
            else if (media instanceof Video)
                url = ((Video) media).getThumbnail();
            httpget = new HttpGet(url.replaceAll(" ", "%20"));
            httpget.setConfig(_requestConfig);
            HttpResponse response = _httpclient.execute(httpget);
            StatusLine status = response.getStatusLine();
            int code = status.getStatusCode();
            if (code < 200 || code >= 300) {
                _logger.error("Failed fetch media item " + id + ". URL=" + url +
                        ". Http code: " + code + " Error: " + status.getReasonPhrase());
                return false;
            }
            HttpEntity entity = response.getEntity();
            if (entity == null) {
                _logger.error("Entity is null for " + id + ". URL=" + url +
                        ". Http code: " + code + " Error: " + status.getReasonPhrase());
                return false;
            }
            InputStream input = entity.getContent();
            byte[] imageContent = IOUtils.toByteArray(input);
            BufferedImage image = ImageIO.read(new ByteArrayInputStream(imageContent));
            int width = image.getWidth();
            int height = image.getHeight();
            if (image != null && ImageUtils.isImageBigEnough(width, height)) {
                ImageVectorization imvec = new ImageVectorization(id, image, targetLengthMax, maxNumPixels);
                if (media instanceof Image) {
                    ((Image) media).setWidth(width);
                    ((Image) media).setHeight(height);
                } else if (media instanceof Video) {
                    ((Video) media).setWidth(width);
                    ((Video) media).setHeight(height);
                }
                ImageVectorizationResult imvr = imvec.call();
                double[] vector = imvr.getImageVector();
                if (vector == null || vector.length == 0) {
                    _logger.error("Error in feature extraction for " + id);
                }
                handler.index(id, vector);
                indexed = true;
            }
        } catch (Exception e) {
            //_logger.error(e.getMessage(), e);

        } finally {
            if (httpget != null) {
                httpget.abort();
            }
            return indexed;
        }
    }

    public boolean createCollection(String name) throws Exception {
        String request = "http://" + Configuration.INDEX_SERVICE_HOST + ":8080/VisualIndexService/rest/visual/add/" + name;
        HttpGet httpget = new HttpGet(request.replaceAll(" ", "%20"));
        httpget.setConfig(_requestConfig);
        HttpResponse response = _httpclient.execute(httpget);
        StatusLine status = response.getStatusLine();
        int code = status.getStatusCode();
        if (code < 200 || code >= 300) {
            _logger.error("Failed create collection with name " + name +
                    ". Http code: " + code + " Error: " + status.getReasonPhrase());
            return false;
        }
        HttpEntity entity = response.getEntity();
        if (entity == null) {
            _logger.error("Entity is null for create collection " + name +
                    ". Http code: " + code + " Error: " + status.getReasonPhrase());
            return false;
        }
        return true;
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
                /*if (mediaItem.getWidth() == null && mediaItem.getHeight() == null) {
                    mediaItem.setSize(image.getWidth(), image.getHeight());
                }*/
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
            return results;
        }
    }

    public static void main(String[] args) throws Exception {
        MorphiaManager.setup("127.0.0.1");
        VisualIndexer v = new VisualIndexer("tree");
        Image im = new Image();
        //im.setId("1235");
        im.setUrl("http://animalia-life.com/data_images/dog/dog4.jpg");
        boolean indexed = v.index(im);
    }
}
