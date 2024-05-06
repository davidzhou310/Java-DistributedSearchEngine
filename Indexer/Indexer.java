package cis5550.jobs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;

public class Indexer {
    private static final Logger logger = Logger.getLogger(cis5550.jobs.Indexer.class);

    public static void run(FlameContext flameContext, String[] args) {

        try {

            // get the url-hashedURL table
            FlameRDD hashedURLRDD = flameContext.fromTable("pt-crawl-debug", row -> {
                if (row == null) {
                    logger.info("Encountered a null row.");
                    return "";
                }
                String hashedURL = row.key();
                return hashedURL;
            });

            logger.info("hashURLRDD finished");

            // convert to flamepair: (word, url:tf)
            FlamePairRDD wordURLTF = hashedURLRDD.flatMapToPair((str) -> {
                KVSClient kvs = flameContext.getKVS();
                List<FlamePair> pairIterator = new ArrayList<>();
                String hashedURL = str;
                Row row = kvs.getRow("pt-crawl-debug", hashedURL);
                String page = row.get("page");
                String url = row.get("url");
                if(page == null || page.equals("") || url == null || url.equals("") || url.contains("..")) {
                    logger.info("No content/page available for URL: " + url);
                    return pairIterator;
                }

                // get the token from the url/////////////////////////////////////////////////////////////////////////////////////////////

                int indexOfSlash = url.lastIndexOf("/");
                if (indexOfSlash!=-1) {
                    String usefulContent = url.substring(indexOfSlash+1);
                    String[] usefulContentArr = usefulContent.split("[_-]");
                    for(String s: usefulContentArr) {
                        page += " "+ s;
                    }
                }
                // process stop words, retain english word
                List<String> tokens = PageParsing.processCleanedPage(page);
                // page has nothing to process
                if(tokens.isEmpty()) {
                    return pairIterator;
                }

                Map<String, Integer> tfMap = new HashMap<>();
                for (String word : tokens) {
                    if(!word.equals("")) {
                        // this is a valid word
                        tfMap.put(word, tfMap.getOrDefault(word, 0)+1);
                    }
                }
                // finished creating the tfMap
                // output the word url and normalized tf pairs
                for (Entry<String, Integer> entry: tfMap.entrySet()) {
                    String word = entry.getKey();
                    int tf = tfMap.get(word);
                    kvs.put("pt-IndexerTwo",word , url, tf+"");
                }
                int max = Collections.max(tfMap.values());
                kvs.put("pt-IndexerTwo","MAX" ,url, max+"");
                return pairIterator;
            });

            logger.info("completed (word, url:tf)");

//            wordURLTF.saveAsTable("pt-TFTab");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}