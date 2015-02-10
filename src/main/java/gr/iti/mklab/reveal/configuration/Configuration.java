package gr.iti.mklab.reveal.configuration;


import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

/**
 * Created by kandreadou on 2/2/15.
 */
public class Configuration {

    public static enum CONF {
        LOCAL, ITI310, DOCKER
    }


    public static String INDEX_FOLDER;
    public static String SCRIPTS_FOLDER;
    public static String CRAWLS_FOLDER;

    public static void main(String[] args) throws Exception {
        Configuration c = new Configuration();
        c.loadConfiguration(CONF.ITI310);
    }

    public static String CRAWLS_DIR;
    public static String VISUAL_DIR;
    public static String LEARNING_FOLDER;
    public static String INDEX_SERVICE_HOST;

    public static void load(String file) throws ConfigurationException {
        PropertiesConfiguration conf = new PropertiesConfiguration(file);
        CRAWLS_DIR = conf.getString("crawlsDir");
        VISUAL_DIR = conf.getString("visualDir");
        LEARNING_FOLDER = conf.getString("learningFolder");
        INDEX_SERVICE_HOST = conf.getString("indexServiceHost");
    }

    public static void loadConfiguration(CONF conf) {
        switch (conf) {
            case LOCAL:
                break;
            case ITI310:
                /*
                learningFolder=/home/iti-310/VisualIndex/learning_files/
                indexFolder=/home/iti-310/VisualIndex/data/
                scriptsFolder=/home/iti-310/vdata/
                crawlsFolder=/home/iti-310/VisualIndex/data/
                 */
                LEARNING_FOLDER = "/home/iti-310/VisualIndex/learning_files/";
                INDEX_FOLDER = "/home/iti-310/VisualIndex/data/";
                SCRIPTS_FOLDER = "/home/iti-310/vdata/";
                CRAWLS_FOLDER = "/home/iti-310/VisualIndex/data/";
                break;
            case DOCKER:
                LEARNING_FOLDER = "/usr/learning_files/";
                INDEX_FOLDER = "/usr/visual/";
                SCRIPTS_FOLDER = "/usr/bubing/";
                CRAWLS_FOLDER = "/usr/crawls/";
                break;
        }

    }
}
