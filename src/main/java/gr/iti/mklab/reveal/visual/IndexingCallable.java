package gr.iti.mklab.reveal.visual;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.concurrent.Callable;

import javax.imageio.ImageIO;

import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.iti.mklab.reveal.util.DisturbingDetectorClient;
import gr.iti.mklab.reveal.util.ImageUtils;
import gr.iti.mklab.simmo.core.items.Image;
import gr.iti.mklab.simmo.core.items.Media;
import gr.iti.mklab.simmo.core.items.Video;
import gr.iti.mklab.visual.vectorization.ImageVectorization;
import gr.iti.mklab.visual.vectorization.ImageVectorizationResult;

public class IndexingCallable implements Callable<IndexingResult> {
	
	protected static int maxNumPixels = 768 * 512;
    protected static int targetLengthMax = 1024;
    private Media media;
	private String collection;
	
    private static Logger _logger = LoggerFactory.getLogger(IndexingCallable.class);
    private static CloseableHttpClient _httpclient;
    private static RequestConfig _requestConfig;
    
    static {
    	 PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
    	 cm.setMaxTotal(50);
    	 cm.setDefaultMaxPerRoute(20);
    	 
    	 _httpclient = HttpClients.custom()
                 .setConnectionManager(cm)
                 .build();
    	 
    	 _requestConfig = RequestConfig.custom()
                 .setSocketTimeout(30000)
                 .setConnectTimeout(30000)
                 .build();
    }
	
	public IndexingCallable(Media media, String collection) {
		this.media = media;
		this.collection = collection;
	}
	
	@Override
	public IndexingResult call() {
		try {
			double[] vector = process();
			return new IndexingResult(media, vector);
		}
		catch(Exception e) {
			_logger.error("Exception for " + media.getId(), e);
			return new IndexingResult(media, null);
		}
	}
	
	public double[] process() {
        HttpGet httpget = null;
        try {
        	
            String id = media.getId();
            String type = null;
            String url = null;
            if (media instanceof Image) {
                url = media.getUrl();
                type = "image";
            }
            else if (media instanceof Video) {
                url = ((Video) media).getThumbnail();
                type = "video";
            }
            else {
            	_logger.error("Unknown instance of " + id);
            }
            
            if(url == null) {
            	_logger.error("Url is null for " + id);
            	return null;
            }
            
            httpget = new HttpGet(url.replaceAll(" ", "%20"));
            httpget.setConfig(_requestConfig);
            
            HttpResponse response = _httpclient.execute(httpget);
            StatusLine status = response.getStatusLine();
            int code = status.getStatusCode();
            HttpEntity entity = response.getEntity();
            
            if (code < 200 || code >= 300) {
                _logger.error("Failed fetch media item " + id + ". URL=" + url + ". Http code: " + code + " Error: " + status.getReasonPhrase());
                EntityUtils.consumeQuietly(entity);
                return null;
            }
            
            if (entity == null) {
                _logger.error("Entity is null for " + id + ". URL=" + url +". Http code: " + code + " Error: " + status.getReasonPhrase());
                EntityUtils.consumeQuietly(entity);
                return null;
            }
            
            InputStream input = entity.getContent();
            Header contentType = entity.getContentType();
            byte[] imageContent = IOUtils.toByteArray(input);
            if(!ImageUtils.checkContentHeaders(imageContent.length, contentType.getValue())) {
            	_logger.error("Checking content and content type failed for id=" + id + ". Content:" + imageContent.length + " bytes, type:" + contentType.getValue());
            	return null;
            }
            
            BufferedImage image = ImageIO.read(new ByteArrayInputStream(imageContent));
            if (ImageUtils.checkImage(image) || (media instanceof Video)) {
                int width = image.getWidth();
                int height = image.getHeight();
            	ImageVectorization imvec = new ImageVectorization(id, image, targetLengthMax, maxNumPixels);
                if (media instanceof Image) {
                    ((Image) media).setWidth(width);
                    ((Image) media).setHeight(height);
                } 
                else if (media instanceof Video) {
                    ((Video) media).setWidth(width);
                    ((Video) media).setHeight(height);
                }
                
                ImageVectorizationResult imvr = imvec.call();
               
                double[] vector = imvr.getImageVector();
                if (vector == null || vector.length == 0) {
                    _logger.error("Error in feature extraction for " + id);
                    return null;
                }
                
                try {
                	if(imageContent != null && imageContent.length > 0) {
                		DisturbingDetectorClient.detect(url, imageContent, collection, id, type);
                	}
                }
                catch(Exception e) {
                	_logger.error("Exception in vectorization for id=" + id + ", url=" + url + ", collection=" + collection, e);
                }
                
                return vector;
            }
        } 
        catch (Exception e) {
            _logger.error(e.getMessage(), e);
        } 
        finally {
            if (httpget != null) {
                httpget.abort();
            }
        }
		return null;
    }

}
