package gr.iti.mklab.reveal.visual;

import gr.iti.mklab.framework.client.search.visual.VisualIndexHandler;
import gr.iti.mklab.reveal.configuration.Configuration;
import gr.iti.mklab.simmo.items.Image;
import gr.iti.mklab.simmo.items.Media;
import gr.iti.mklab.simmo.items.Video;
import gr.iti.mklab.simmo.morphia.MediaDAO;
import gr.iti.mklab.simmo.morphia.MorphiaManager;
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


/**
 * Created by kandreadou on 2/10/15.
 */
public class VisualIndexer {

    protected static int maxNumPixels = 768 * 512;
    protected static int targetLengthMax = 1024;
    protected static PCA pca;
    // One VisualIndexHandler per collection
    private VisualIndexHandler handler;
    private CloseableHttpClient _httpclient;
    private RequestConfig _requestConfig;
    private final static Logger _logger = LoggerFactory.getLogger(VisualIndexer.class);
    private MediaDAO<Image> imageDAO;

    public VisualIndexer(String collectionName) {
        try {
            Configuration.load("local.properties");
            handler = new VisualIndexHandler("http://" + Configuration.INDEX_SERVICE_HOST + ":8080/VisualIndexService", collectionName);
            imageDAO = new MediaDAO<>(Image.class, collectionName);
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
            _requestConfig = RequestConfig.custom()
                    .setSocketTimeout(30000)
                    .setConnectTimeout(30000)
                    .build();
            PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
            _httpclient = HttpClients.custom()
                    .setConnectionManager(cm)
                    .build();
        } catch (Exception ex) {
            //TODO: do something
        }
    }

    public boolean index(Media media, String collectionName) {
        boolean indexed = false;
        if (handler == null)
            throw new IllegalStateException("There is no index for the collection " + collectionName);
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
                return indexed;
            }
            HttpEntity entity = response.getEntity();
            if (entity == null) {
                _logger.error("Entity is null for " + id + ". URL=" + url +
                        ". Http code: " + code + " Error: " + status.getReasonPhrase());
                return indexed;
            }
            InputStream input = entity.getContent();
            byte[] imageContent = IOUtils.toByteArray(input);
            BufferedImage image = ImageIO.read(new ByteArrayInputStream(imageContent));
            if (image != null) {
                ImageVectorization imvec = new ImageVectorization(id, image, targetLengthMax, maxNumPixels);
                /*if (mediaItem.getWidth() == null && mediaItem.getHeight() == null) {
                    mediaItem.setSize(image.getWidth(), image.getHeight());
                }*/
                ImageVectorizationResult imvr = imvec.call();
                double[] vector = imvr.getImageVector();
                if (vector == null || vector.length == 0) {
                    _logger.error("Error in feature extraction for " + id);
                }
                indexed = handler.index(id, vector);
                if (indexed)
                    imageDAO.save((Image) media);

            }
        } catch (Exception e) {
            _logger.error(e.getMessage(), e);

        } finally {
            if (httpget != null) {
                httpget.abort();
            }
            return indexed;
        }
    }

    public static void main(String[] args){
        MorphiaManager.setup("127.0.0.1");
        VisualIndexer v = new VisualIndexer("tree");
        Image im = new Image();
        im.setId("1235");
        im.setUrl("http://animalia-life.com/data_images/dog/dog4.jpg");
        boolean indexed = v.index(im, "test");
    }
}
