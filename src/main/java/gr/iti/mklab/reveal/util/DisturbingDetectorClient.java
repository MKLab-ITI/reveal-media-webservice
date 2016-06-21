package gr.iti.mklab.reveal.util;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;

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

	private Logger _logger = Logger.getLogger(DisturbingDetectorClient.class);
	
	private HttpClient httpClient;
	private String webServiceHost;
	
	public DisturbingDetectorClient(String webServiceHost) {
	        this.webServiceHost = webServiceHost;
	        MultiThreadedHttpConnectionManager cm = new MultiThreadedHttpConnectionManager();
	        HttpConnectionManagerParams params = new HttpConnectionManagerParams();
	        params.setMaxTotalConnections(100);
	        params.setDefaultMaxConnectionsPerHost(20);
	        params.setConnectionTimeout(30000);
	        params.setSoTimeout(30000);
	        cm.setParams(params);
	        this.httpClient = new HttpClient(cm);
	}
	
    public double detect(String url, byte[] image) {

        PostMethod postMethod = null;
        try {
            ByteArrayPartSource source = new ByteArrayPartSource("bytes", image);
            Part[] parts = {
                    new StringPart("url", url),
                    new FilePart("bytearray", source)
            };
            
            postMethod = new PostMethod(webServiceHost + "/disturbingdetector/disturbingdetector/getfrombytearray");
            postMethod.setRequestEntity(new MultipartRequestEntity(parts, postMethod.getParams()));
            int code = httpClient.executeMethod(postMethod);
            if (code == 200) {
                InputStream responseStream = postMethod.getResponseBodyAsStream();
                String raw = IOUtils.toString(responseStream);
                raw = raw.replaceAll("\"", "");
                
                _logger.info(url + " => " + raw);
                
                double disturbanceValue = Double.parseDouble(raw);
                return disturbanceValue;
                
             
            } else {
                _logger.error("Http returned code: " + code);
            }
        } catch (Exception e) {
            _logger.error("Exception for url: " + url, e);
            e.printStackTrace();
        } finally {
            if (postMethod != null) {
            	postMethod.releaseConnection();
            }
        }
        return -10000;
    }
    
	public static void main(String[] args) throws IOException {
		URL url = new URL("https://s3-eu-west-1.amazonaws.com/spiked-online.com/images/baltimore_riots.jpg");
		byte[] imageBytes = IOUtils.toByteArray((url).openStream());
		
		
		DisturbingDetectorClient dd = new DisturbingDetectorClient("http://xxx.xxx.xxx.xxx:8080");
		dd.detect(url.toString(), imageBytes);
	}

}
