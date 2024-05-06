package cis5550.jobs;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import KVS.kvs.KVSClient;
import KVS.kvs.Row;
//import cis5550.kvs.KVSClient;
//import cis5550.kvs.Row;

import cis5550.tools.Logger;

public class ProcessQuery {
    private static final Logger logger = Logger.getLogger(cis5550.jobs.ProcessQuery.class);
    public static class URLScoreCard implements Comparable<URLScoreCard>{
        String url;
        double score;
        public URLScoreCard(String url, double score) {
            this.url = url;
            this.score = score;
        }
        @Override
        public int compareTo(URLScoreCard other) {
            URLScoreCard urlScoreCard = other;
            return Double.compare(urlScoreCard.score, this.score); // reverse order
        }
        @Override
        public String toString() {
            return this.url;
        }
    }

    public static List<String> getResults(String ipPort, String[] keywords) throws IOException {
        System.out.println(keywords.toString());
        KVSClient kvs = new KVSClient(ipPort);
        // get the total number of documents
        int N = kvs.count("pt-TFTable");
        Set<String> queryWords = new HashSet<>();
        for (int i = 0; i < keywords.length; i++) {
            // this is a valid word
            queryWords.add(keywords[i]);
        }
        // save url and its score in a map
        Map<String, Double> urlScore = new HashMap<>();
        for (String keyWord : queryWords) {
            try {
                // find the urls with matching words
                Row tfRow = kvs.getRow("pt-final", keyWord);
                if (tfRow == null){
                    System.out.println("Keyword \"" + keyWord + "\" does not exist.");
                    continue;
                }
                // get the number of docs that contain this keyWord
                double kwNumDocs = tfRow.columns().size();
                double idf = Math.log(N / kwNumDocs);
                for (String url : tfRow.columns()){
                    double tf = Double.parseDouble(tfRow.get(url));
                    urlScore.put(url, urlScore.getOrDefault(url, 0.0) + tf * idf);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

//        // add pagerank to each url's score
//        for (String url : urlScore.keySet()) {
//            double pageRank = 0; //TODO
//            try {
//                Row pageRankRow = kvs.getRow("pt-pageranks", Hasher.hash(url));
//
//                if (pageRankRow != null) {
//                    pageRank = Double.parseDouble(pageRankRow.get("rank"));
//                    urlScore.put(url, urlScore.get(url) + pageRank);
//                }
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }

        // now have a map of all scores
        // make a list of scorecards to sort
        List<URLScoreCard> urlScoreCards = new ArrayList<>();
        for (Entry<String, Double> entry: urlScore.entrySet()) {
            URLScoreCard toadd = new URLScoreCard(entry.getKey(), entry.getValue());
            urlScoreCards.add(toadd);
        }
        // sort
        Collections.sort(urlScoreCards);
        int limit = Math.min(urlScoreCards.size(), 50);
        List<URLScoreCard> topUrlScoreCards = new ArrayList<>(urlScoreCards.subList(0, limit));
        List<String> urlResult = new ArrayList<>();
        for (URLScoreCard card: topUrlScoreCards) {
            logger.info(card + "");
            System.out.println(card);
            urlResult.add(card.url);
        }
        return urlResult;
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.out.print("Please provide Coordinator's Ip:Port and search words");
            return;
        }
        if (args.length < 2){
            System.out.print("Please provide search words");
            return;
        }
        logger.info("inside query process");

        List<String> searchRes = getResults(args[0], Arrays.copyOfRange(args, 1, args.length));
        System.out.println(searchRes.toString());


    }
}
