package gr.iti.mklab.reveal.util;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.multipart.ByteArrayPartSource;
import org.apache.commons.httpclient.methods.multipart.FilePart;
import org.apache.commons.httpclient.methods.multipart.MultipartRequestEntity;
import org.apache.commons.httpclient.methods.multipart.Part;
import org.apache.commons.httpclient.methods.multipart.StringPart;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

public class DisturbingDetectorClient {

	private static Logger _logger = Logger.getLogger(DisturbingDetectorClient.class);
	
	private static HttpClient httpClient;
	private static String webServiceHost;
	
	public static void initialize(String webServiceHost) {
		DisturbingDetectorClient.webServiceHost = webServiceHost;
		MultiThreadedHttpConnectionManager cm = new MultiThreadedHttpConnectionManager();
		HttpConnectionManagerParams params = new HttpConnectionManagerParams();
		params.setMaxTotalConnections(100);
		params.setDefaultMaxConnectionsPerHost(20);
		params.setConnectionTimeout(30000);
		params.setSoTimeout(30000);
		cm.setParams(params);
		httpClient = new HttpClient(cm);    
	}
	
    public static String detect(String url, byte[] image, String collection, String id, String type) {

    	if(webServiceHost == null || httpClient == null) {
    		return "Null http client";
    	}
    	
        PostMethod postMethod = null;
        try {
            ByteArrayPartSource source = new ByteArrayPartSource("bytes", image);
            Part[] parts = {
                    new StringPart("url", url),
                    new FilePart("bytearray", source),
                    new StringPart("collection", collection),
                    new StringPart("id", id),
                    new StringPart("type", type)
            };
            
            postMethod = new PostMethod(webServiceHost + "/disturbingdetector/disturbingdetector/getfrombytearray");
            postMethod.setRequestEntity(new MultipartRequestEntity(parts, postMethod.getParams()));
            int code = httpClient.executeMethod(postMethod);
            if (code == 200) {
                InputStream responseStream = postMethod.getResponseBodyAsStream();
                String raw = IOUtils.toString(responseStream);
               
                if(raw != null && raw.equals("ok")) {
                	return raw;
                }
           
            } else {
                
                InputStream responseStream = postMethod.getResponseBodyAsStream();
                String raw = IOUtils.toString(responseStream);
                
                _logger.error("Http returned code: " + code + " for " + url + " in collection " + collection + ". Message: " + raw);
            
                return "Error in code - " + code;
            }
            
        } catch (Exception e) {
            _logger.error("Exception for url: " + url, e);
            e.printStackTrace();
        } finally {
            if (postMethod != null) {
            	postMethod.releaseConnection();
            }
        }
        
        return "DisturbingDetectorClienteError for id=" + id;
    }
    
	public static void main(String[] args) throws IOException {
		URL url = new URL("https://s3-eu-west-1.amazonaws.com/spiked-online.com/images/baltimore_riots.jpg");
		byte[] imageBytes = IOUtils.toByteArray((url).openStream());
		
		
		 DisturbingDetectorClient.initialize("http://xxx.xxx.xxx.xxx:8080");
		 DisturbingDetectorClient.detect(url.toString(), imageBytes, "test", "1", "image");
	}

}
