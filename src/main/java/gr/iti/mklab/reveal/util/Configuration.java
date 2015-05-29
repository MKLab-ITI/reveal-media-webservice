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
    public static String VISUAL_DIR;
    public static String LEARNING_FOLDER;
    public static String INDEX_SERVICE_HOST;
    public static String MONGO_HOST;
    public static boolean ADD_SOCIAL_MEDIA;
    public static String STREAM_CONF_FILE;
    public static String GEO_CONF_FILE;
    public static String MANIPULATION_REPORT_PATH;
    public static boolean PUBLISH_RABBITMQ;

    public static void load(String file) throws ConfigurationException {
        PropertiesConfiguration conf = new PropertiesConfiguration(file);
        CRAWLS_DIR = conf.getString("crawlsDir");
        VISUAL_DIR = conf.getString("visualDir");
        LEARNING_FOLDER = conf.getString("learningFolder");
        INDEX_SERVICE_HOST = conf.getString("indexServiceHost");
        MONGO_HOST = conf.getString("mongoHost");
    }

    public static void load(InputStream stream) throws ConfigurationException, IOException {
        Properties conf = new Properties();
        conf.load(stream);
        CRAWLS_DIR = conf.getProperty("crawlsDir");
        VISUAL_DIR = conf.getProperty("visualDir");
        LEARNING_FOLDER = conf.getProperty("learningFolder");
        INDEX_SERVICE_HOST = conf.getProperty("indexServiceHost");
        MONGO_HOST = conf.getProperty("mongoHost");
        ADD_SOCIAL_MEDIA = Boolean.valueOf(conf.getProperty("getSocialMedia"));
        STREAM_CONF_FILE = conf.getProperty("streamConfFile");
        GEO_CONF_FILE = conf.getProperty("geoConfFile");
        MANIPULATION_REPORT_PATH = conf.getProperty("manipulationReportPath");
        PUBLISH_RABBITMQ = Boolean.parseBoolean(conf.getProperty("publish"));
    }
}
