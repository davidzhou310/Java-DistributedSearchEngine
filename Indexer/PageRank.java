package cis5550.jobs;

import Flame.flame.FlameContext;
import Flame.flame.FlamePair;
import Flame.flame.FlamePairRDD;
import Flame.flame.FlameRDD;

//import cis5550.flame.FlameContext;
//import cis5550.flame.FlamePair;
//import cis5550.flame.FlamePairRDD;
//import cis5550.flame.FlameRDD;

import cis5550.tools.Hasher;
import cis5550.tools.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import static java.util.Arrays.copyOfRange;

public class PageRank {
    private static final Logger logger = Logger.getLogger(cis5550.jobs.PageRank.class);
    public static void run(FlameContext ctx, String[] threshold) throws Exception {
        logger.info("Running pagerank...");
        ctx.output("Running pagerank...");
        System.out.println("Running pagerank...");
        double t = 0.1;
        try {
            t = Double.parseDouble(threshold[0]); // convergence threshold
        } catch (NumberFormatException e) {
            System.out.println("Please enter a double for convergence threshold");
            return;
        }
        FlamePairRDD stateTable = ctx.fromTable("pt-crawl", row -> {
            String page = row.get("page");
            String url = row.get("url");
            String urlHashed = row.key(); // hashed url
            if (page == null || url == null) {
                return null;
            }
            HashSet<String> normalizedURLSet = new HashSet<>(); // hashed
            List<String> normalizedURL = GenerateLinksFromPage.extractAndNormalizeURLs(url, page);
            for (String u : normalizedURL) {
                normalizedURLSet.add(Hasher.hash(u));
            }
            // HashSet<String> normalizedURLSet = new HashSet<>(normalizedURL);
            if (normalizedURLSet.isEmpty()) {
                return null;
            }
            // System.out.println("URL: " + url);
            return urlHashed + ";1.0,1.0," + String.join(",", normalizedURLSet);
        }).mapToPair(s -> new FlamePair(s.split(";")[0], s.split(";")[1]));

        int iter = 0;
        while (true) {
            logger.info("Iteration " + iter++);
            // distribute each url's vote (rc) evenly to links on its page
            FlamePairRDD transferTable = stateTable.flatMapToPair(p -> {
                String currentUrl = p._1(); // hashed
                // System.out.println("current url: " + currentUrl);
                String[] rankOutUrls = p._2().split(",");
                // System.out.println("p1: " + p._1() + " p2: " + p._2());
                double rc = Double.parseDouble(rankOutUrls[0]);
                String[] outUrls = copyOfRange(rankOutUrls, 2, rankOutUrls.length);
                ArrayList<FlamePair> pairs = new ArrayList<>();
                // if current rank is 0 or the page does not have any outward links (sink), return itself and 0
                if (rc == 0 || outUrls.length == 0) {
                    pairs.add(new FlamePair(currentUrl, "0"));
                    return pairs;
                }
                // Calculate the number of each linked url's received vote from current url
                double vote = 0.85 * rc / outUrls.length;
                for (String outUrl : outUrls) {
                    pairs.add(new FlamePair(outUrl, "" + vote));
                }
                return pairs;
            });
            // count the number of total votes each url has got from the previous distribution
            FlamePairRDD aggTransfer = transferTable.foldByKey("0", (vote1, vote2) ->
                    String.valueOf(Double.parseDouble(vote1) + Double.parseDouble(vote2))
            );
            transferTable.destroy();

            // update state table
            FlamePairRDD updatedState = stateTable.join(aggTransfer).flatMapToPair(p -> {
                String url = p._1();
                String[] cols = p._2().split(",");
                double oldRc = Double.parseDouble(cols[0]);
                Double newRc = 0.15 + Double.parseDouble(cols[cols.length - 1]);
                // get all the outward links (hashed)
                ArrayList<String> links = new ArrayList<>();
                for (int i = 2; i < cols.length - 1; i++) {
                    links.add(cols[i]);
                }
                // replace old rp with old rc (new rp = old rc)
                // put aggTransfer's column to new rc (new rc = aggregate from aggTransfer)
                ArrayList<FlamePair> pairs = new ArrayList<>();
                pairs.add(new FlamePair(url, newRc + "," + oldRc + "," + String.join(",", links)));
                return pairs;
            });
            // stateTable.destroy();
            aggTransfer.destroy();

            // compute change in ranks
            FlameRDD rankDiffs = updatedState.flatMap(p -> {
                double rc = Double.parseDouble(p._2().split(",")[0]); // current rank
                double rp = Double.parseDouble(p._2().split(",")[1]); // previous rank
                ArrayList<String> strList = new ArrayList<>();
                strList.add(String.valueOf(Math.abs(rc - rp)));
                return strList;
            });
            // (vertically) fold the differences to find the maximum difference
            double maxDiff = Double.parseDouble(rankDiffs.fold("0", (d1, d2) ->
                    "" + Math.max(Double.parseDouble(d1), Double.parseDouble(d2))));
            if (maxDiff < t) {
                break;
            } else {
                stateTable = updatedState;
            }
        }

        // put ranks to table pt-pageranks
        stateTable.flatMapToPair(p -> {
            String urlHashed = p._1();
            String finalRank = p._2().split(",")[0];
            // put to table pt-pageranks
            ctx.getKVS().put("pt-pageranks", urlHashed, "rank", finalRank);
            return new ArrayList<FlamePair>();
        });
    }
}



