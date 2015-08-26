package gr.iti.mklab.reveal;

import gr.demokritos.iit.api.API;
import gr.demokritos.iit.ner.NamedEntityList;
import gr.demokritos.iit.re.RelationCounter;
import gr.demokritos.iit.re.RelationList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 * Created by kandreadou on 8/25/15.
 */
public class DemoCerth {

    public static void main(String[] args) {

        List<String> texts = Arrays.asList(
                "@groovydragon My horse has caught fire in Syria because of the #heat",
                "@bluewave @barbiedoll I looked at my green fingers with delight",
                "Obama accused Tony Blair of being the British prime minister",
                "Barack Obama accused Tony Blair of being the British prime minister",
                "Lara Croft denied George Orwell",
                "George Orwell killed Lara Croft in the battle of Albaquerque");

        System.out.println("------- Print named entities without DBpedia ----------\n");
        // switched to hashmap to clearly maintain mapping between text and
        // named entity list - entities found in that text
        HashMap<String, NamedEntityList> entities =
                API.NER(texts, API.FORMAT.TEXT_CERTH_TWEET, false);
        for(NamedEntityList entitylist: entities.values()){
            entitylist.prettyPrint();
        }
        System.out.println("------- Print named entities with DBpedia ----------\n");
        entities = API.NER(texts, API.FORMAT.TEXT_CERTH_TWEET, true);
        for(NamedEntityList entitylist: entities.values()){
            entitylist.prettyPrint();
        }
        System.out.println("------- Print Relation Counter ----------\n");
        // use new api - pass hashmap mapping of texts to entity lists
        // so that we don't run ner again
        RelationCounter counter = API.RE(entities, API.FORMAT.TEXT_CERTH_TWEET);
        counter.prettyPrint();
        // Give example on how we can see all the relations used in the counter
        // counter contains groups - hashmap from relation label to relation list
        System.out.println("Relation list traversal");
        Set <String> labels = counter.getLabels();
        for (String label : labels){
            RelationList rl = counter.getGroup(label);
            rl.prettyPrint();
        }
    }
}
