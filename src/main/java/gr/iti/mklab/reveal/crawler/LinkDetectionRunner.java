package gr.iti.mklab.reveal.crawler;

import gr.iti.mklab.simmo.core.documents.Webpage;
import gr.iti.mklab.simmo.core.morphia.ObjectDAO;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

/**
 * Created by kandreadou on 5/4/15.
 */
public abstract class LinkDetectionRunner {

    private long LAST_CALL = System.currentTimeMillis();
    private static int LAST_POSITION = 0;
    private static final int STEP = 1000;
    private ObjectDAO<Webpage> pageDAO;
    private static boolean isRunning = true;

    public LinkDetectionRunner(String collection) throws ExecutionException {
        pageDAO = new ObjectDAO<>(Webpage.class, collection);
    }

    public void enqueueLinks() {
        if(System.currentTimeMillis() - LAST_CALL>10000) {
            LAST_CALL = System.currentTimeMillis();
            List<Webpage> pageList;
            Pattern pattern = Pattern.compile("^Twitter", Pattern.CASE_INSENSITIVE);
            //pageList = pageDAO.getItems((int) pageDAO.count() - LAST_POSITION, LAST_POSITION);
            pageList = pageDAO.getDatastore().find(Webpage.class).field("_id").equal(pattern).offset(LAST_POSITION).limit(STEP).asList();
            System.out.println("LAST_POSITION " + LAST_POSITION + " pageList.size " + pageList.size());
            while (!pageList.isEmpty() && isRunning) {
                for (Webpage page : pageList) {
                    if (page.getId().startsWith("Twitter"))
                        processLink(page.getUrl());
                }
                LAST_POSITION += STEP;
                pageList = pageDAO.getDatastore().find(Webpage.class).field("_id").equal(pattern).offset(LAST_POSITION).limit(STEP).asList();
            }
        }
    }

    public abstract void processLink(String link);

    public static void main(String[] args) throws Exception {
        /*Configuration.load("local.properties");
        MorphiaManager.setup("127.0.0.1");
        LinkDetectionRunner runner = new LinkDetectionRunner("texasshooting4");
        Thread t = new Thread(runner);
        t.start();*/
    }
}
