package gr.iti.mklab.reveal.util;

import com.google.gson.Gson;
import eu.socialsensor.framework.common.domain.WebPage;

import java.net.URL;

/**
 * Created by kandreadou on 10/6/14.
 */
public class MediaItem extends eu.socialsensor.framework.common.domain.MediaItem {

    public MediaItem(URL url, WebPage page) {
        super(url, page);
    }

    @Override
    public String toJSONString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }
}
