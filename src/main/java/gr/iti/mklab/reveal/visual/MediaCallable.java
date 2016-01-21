package gr.iti.mklab.reveal.visual;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.concurrent.Callable;

import javax.imageio.ImageIO;

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

import gr.iti.mklab.reveal.util.ImageUtils;
import gr.iti.mklab.simmo.core.items.Image;
import gr.iti.mklab.simmo.core.items.Media;
import gr.iti.mklab.simmo.core.items.Video;
import gr.iti.mklab.visual.vectorization.ImageVectorization;
import gr.iti.mklab.visual.vectorization.ImageVectorizationResult;

public class MediaCallable implements Callable<MediaCallableResult> {
	
	protected static int maxNumPixels = 768 * 512;
    protected static int targetLengthMax = 1024;
    private Media media;
    private static Logger _logger = LoggerFactory.getLogger(MediaCallable.class);
    private static CloseableHttpClient _httpclient;
    private static RequestConfig _requestConfig;
    static{
    	 PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
    	 _httpclient = HttpClients.custom()
                 .setConnectionManager(cm)
                 .build();
    	 _requestConfig = RequestConfig.custom()
                 .setSocketTimeout(30000)
                 .setConnectTimeout(30000)
                 .build();
    }
	
	public MediaCallable(Media media){
		this.media = media;
	}
	
	@Override
	public MediaCallableResult call() throws Exception {
		double[] vector = process();
		return new MediaCallableResult(media, vector);
	}
	
	public double[] process() {
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
                return null;
            }
            HttpEntity entity = response.getEntity();
            if (entity == null) {
                _logger.error("Entity is null for " + id + ". URL=" + url +
                        ". Http code: " + code + " Error: " + status.getReasonPhrase());
                return null;
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
                imvec.setDebug(true);
                System.out.println("beginning vectorization");
                ImageVectorizationResult imvr = imvec.call();
                System.out.println("Vectorization called properly");
                double[] vector = imvr.getImageVector();
                System.out.println("imvr collected properly");
                if (vector == null || vector.length == 0) {
                    _logger.error("Error in feature extraction for " + id);
                    return null;
                }
                return vector;
            }
        } catch (Exception e) {
            _logger.error(e.getMessage(), e);

        } finally {
            if (httpget != null) {
                httpget.abort();
            }
        }
		return null;
    }

}