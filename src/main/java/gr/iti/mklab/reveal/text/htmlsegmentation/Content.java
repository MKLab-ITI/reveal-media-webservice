package gr.iti.mklab.reveal.text.htmlsegmentation;

import java.util.ArrayList;

/**
 * @author akrithara
 * @version 1.0
 *
 */

public class Content {

    private final String title;
    private final String text;
//    private final String image;
    private final ArrayList<String> images;

    public Content(String title, String text, ArrayList<String> images) {
        this.title = title;
        this.text = text;
//        this.image = image;
        this.images = images;
    }

    public String getText() {
        return text;
    }

    public String getTitle() {
        return title;
    }

//    public String getImage() {
//        return image;
//    }
    
    public ArrayList<String> getImages(){
        return images;
    }

    @Override
    public String toString() {
        return "Content [title=" + title + ", text=" + text + "images=" + images.toString() + "]";
    }
    
}
