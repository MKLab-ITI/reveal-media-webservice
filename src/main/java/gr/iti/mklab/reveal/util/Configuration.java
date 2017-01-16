package gr.iti.mklab.reveal.util;


import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by kandreadou on 2/2/15.
 */
public class Configuration {

    public static String CRAWLS_DIR;
    public static String LEARNING_FOLDER;
    public static String INDEX_SERVICE_HOST;
    public static String STREAM_MANAGER_SERVICE_HOST;
    public static String MONGO_HOST;
    public static boolean ADD_SOCIAL_MEDIA;
    public static boolean PUBLISH_RABBITMQ;
    public static int NUM_CRAWLS;
    public static String DISTURBING_DETECTOR_HOST;
    
    public static double VISUAL_WEIGHT;
    public static double TEXTUAL_WEIGHT;
    public static double DISTANCE_THRESHOLD;
    
    public static void load(String file) throws ConfigurationException {
        PropertiesConfiguration conf = new PropertiesConfiguration(file);
        CRAWLS_DIR = conf.getString("crawlsDir");
        LEARNING_FOLDER = conf.getString("learningFolder");
        INDEX_SERVICE_HOST = conf.getString("indexServiceHost");
        STREAM_MANAGER_SERVICE_HOST = conf.getString("streamManagerServiceHost");
        MONGO_HOST = conf.getString("mongoHost");
        DISTURBING_DETECTOR_HOST=conf.getString("disturbingDetectorHost");
        
        VISUAL_WEIGHT = Double.parseDouble(conf.getString("visualWeight", "0.62"));
        TEXTUAL_WEIGHT = Double.parseDouble(conf.getString("textualWeight", "0.85"));
        DISTANCE_THRESHOLD = Double.parseDouble(conf.getString("distanceThreshold", "0.68"));
    }

    public static void load(InputStream stream) throws ConfigurationException, IOException {
        Properties conf = new Properties();
        conf.load(stream);
        CRAWLS_DIR = conf.getProperty("crawlsDir");
        LEARNING_FOLDER = conf.getProperty("learningFolder");
        INDEX_SERVICE_HOST = conf.getProperty("indexServiceHost");
        STREAM_MANAGER_SERVICE_HOST = conf.getProperty("streamManagerServiceHost");
        MONGO_HOST = conf.getProperty("mongoHost");
        ADD_SOCIAL_MEDIA = Boolean.valueOf(conf.getProperty("getSocialMedia"));
        PUBLISH_RABBITMQ = Boolean.parseBoolean(conf.getProperty("publish"));
        NUM_CRAWLS = Integer.parseInt(conf.getProperty("numCrawls", "2"));
        DISTURBING_DETECTOR_HOST=conf.getProperty("disturbingDetectorHost");
        
        VISUAL_WEIGHT = Double.parseDouble(conf.getProperty("visualWeight", "0.62"));
        TEXTUAL_WEIGHT = Double.parseDouble(conf.getProperty("textualWeight", "0.85"));
        DISTANCE_THRESHOLD = Double.parseDouble(conf.getProperty("distanceThreshold", "0.68"));
    }
}
