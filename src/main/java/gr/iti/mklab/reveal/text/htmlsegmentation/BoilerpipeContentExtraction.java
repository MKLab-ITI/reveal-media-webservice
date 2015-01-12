package gr.iti.mklab.reveal.text.htmlsegmentation;

import java.net.URL;
import java.util.Collections;
import java.util.List;

import de.l3s.boilerpipe.BoilerpipeExtractor;
import de.l3s.boilerpipe.BoilerpipeProcessingException;
import de.l3s.boilerpipe.document.TextDocument;
import de.l3s.boilerpipe.extractors.ArticleExtractor;
import de.l3s.boilerpipe.extractors.CommonExtractors;
import de.l3s.boilerpipe.sax.BoilerpipeSAXInput;
import de.l3s.boilerpipe.sax.HTMLDocument;
import de.l3s.boilerpipe.sax.HTMLFetcher;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

import org.apache.commons.io.IOUtils;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * @author akrithara
 * @version 1.0
 */

public class BoilerpipeContentExtraction {

    public Content contentFromURL(String url) {
        try {
            final HTMLDocument htmlDoc = HTMLFetcher.fetch(new URL(url));
            final TextDocument doc = new BoilerpipeSAXInput(htmlDoc.toInputSource()).getTextDocument();
            String title = doc.getTitle();

            String content = ArticleExtractor.INSTANCE.getText(doc);

            final BoilerpipeExtractor extractor = CommonExtractors.ARTICLE_EXTRACTOR;
            final ImageExtractor ie = ImageExtractor.INSTANCE;

            List<Image> images = ie.process(new URL(url), extractor);

            Collections.sort(images);
            ArrayList<String> image_list = new ArrayList<>();

            images.stream().forEach((i) -> {
                image_list.add(i.getSrc());
            });

            return new Content(title, content, image_list);
        } catch (IOException | SAXException | BoilerpipeProcessingException e) {
            return null;
        }

    }

    /**
     * @param html
     * @return
     * @throws de.l3s.boilerpipe.BoilerpipeProcessingException
     * @throws java.io.IOException
     */
    public Content contentFromHTML(String html) throws BoilerpipeProcessingException, IOException {
        try {
            final InputStream is = IOUtils.toInputStream(html);
            final TextDocument doc = new BoilerpipeSAXInput(new InputSource(is)).getTextDocument();
            String title = doc.getTitle();
            String content = ArticleExtractor.INSTANCE.getText(doc);

            final ImageExtractor ie = ImageExtractor.INSTANCE;

            List<Image> images = ie.process(doc, html);

            Collections.sort(images);
            ArrayList<String> image_list = new ArrayList<>();

            images.stream().forEach((i) -> {
                image_list.add(i.getSrc());
            });

            return new Content(title, content, image_list);
            //return new Content(title, content, image_list);
        } catch (SAXException | BoilerpipeProcessingException e) {
            return null;
        }
    }

}
