package cis5550.jobs;

import cis5550.flame.*;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;
import cis5550.tools.URLParser;
import cis5550.webserver.*;
import cis5550.kvs.*;


import javax.swing.text.Document;
import java.io.*;
import java.net.*;
import java.nio.Buffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


public class Crawler {

    private static final Logger logger = Logger.getLogger(Crawler.class);
//    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);


    static ConcurrentLinkedQueue<String> robotParsed = new ConcurrentLinkedQueue<>();

    public static void run(FlameContext flameContext, String[] urls) {
        if (urls == null || urls.length < 1) {
            flameContext.output("Error: no seed url");
            return;
        } else {
            flameContext.output("OK");
        }


        List<String> seedUrls = new ArrayList<>();
        for (String seed : urls) {
            List<String> normalizedSeed = normalizeUrls(Arrays.asList(seed), seed);
            if (!normalizedSeed.isEmpty()) {
                seedUrls.add(normalizedSeed.get(0));
            }
        }

        FlameRDD urlQueue = null;

        try {
            urlQueue = flameContext.parallelize(seedUrls);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        while (true) {
            try {
                if (!(urlQueue.count() != 0)) break;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

//            FlameRDD finalUrlQueue = urlQueue;
//            scheduler.scheduleAtFixedRate(() -> logQueueContents(finalUrlQueue), 0, 10, TimeUnit.MINUTES);
            logQueueContents(urlQueue);


            try {
                urlQueue = urlQueue.flatMap(url -> {
                    String rowKey = String.valueOf(Hasher.hash(url));
                    KVSClient client = new KVSClient(flameContext.getKVS().getCoordinator());
                    if (client.existsRow("pt-crawl", rowKey)) {
                        return new ArrayList<String>();
                    }
                    List<String> newUrls = new ArrayList<>();
                    URL link = null;
                    try {
                        URI uri = new URI(url);
                        link = uri.toURL();
                    } catch (Exception e) {
                        e.getMessage();
                    }

                    if (link == null) {
                        return new ArrayList<String>();
                    }


                    String host = link.getHost();
                    String path = link.getPath();

                    double crawlDelay = 0.01;

                    if (!isUrlAllowed(client, host, path)) {
//                        System.out.println("URL disallowed: " + url);
                        return new ArrayList<>();
                    }

                    long lastAccessTime = getLastAccessTime(client, host);
                    long currTime = System.currentTimeMillis();
                    if (currTime - lastAccessTime < crawlDelay) {
                        return Arrays.asList(url);
                    }
                    updatelastAccessTime(client, host, currTime);
                    HttpURLConnection headConn;
                    try {
                        headConn = (HttpURLConnection) link.openConnection();

                        headConn.setRequestMethod("HEAD");
                        headConn.setInstanceFollowRedirects(false);
                        headConn.setRequestProperty("User-agent", "cis5550-crawler");

                        headConn.connect();
                    } catch (Exception e) {
                        e.getMessage();
                        return new ArrayList<>();
                    }

                    String contentType = null;
                    String contentLength = null;

                    Row row = new Row(rowKey);
                    row.put("url", url);

                    int responseCode = headConn.getResponseCode();


                    if (responseCode == 200) {

                        contentType = headConn.getContentType();
                        contentLength = String.valueOf(headConn.getContentLength());

                        if (contentType != null) {
                            row.put("contentType", contentType);
                        }
                        if (contentLength != null) {
                            row.put("length", contentLength);
                        }


                        if (contentType != null && contentType.contains("text/html") ) {
                            HttpURLConnection conn = (HttpURLConnection) link.openConnection();
                            conn.setRequestMethod("GET");
                            conn.setRequestProperty("User-agent", "cis5550-crawler");
                            conn.connect();
                            responseCode = conn.getResponseCode();
                            if (responseCode == 200) {
                                InputStream input = conn.getInputStream();
                                byte[] arr = input.readAllBytes();
                                String pageContent = new String(arr, StandardCharsets.UTF_8);

                                List<String> extractedURL = extractURL(pageContent);
                                List<String> normalizedUrls = null;
                                try {
                                    normalizedUrls = normalizeUrls(extractedURL, url);
                                } catch (Exception e) {
                                    System.out.println("normalization error: " + url + "\n" + extractedURL);
                                    e.getMessage();
                                }
                                if (normalizedUrls == null) {
                                    return new ArrayList<>();
                                }

//                                System.out.println("extractedURL: " + extractedURL);
//                                System.out.println("BaseURL: " + url);
//                                pageContent = pageContent.replaceAll("<[^>]*>", " ");
//                                pageContent = pageContent.replaceAll("[^a-zA-Z0-9\\s\\r\\n\\t]", " ");

                                // pageContent = pageContent.replaceAll("(?i)<script[^>]*>(\\S*?)</script>", "");
                                // pageContent = pageContent.replaceAll("(?i)<style[^>]*>(\\S*?)</style>", "");
                                // pageContent = pageContent.replaceAll("<[^>]+>", " ");
                                // pageContent = pageContent.replaceAll("&[^;\\s]+;", " ");
                                try {
//                                    pageContent = pageContent.replaceAll("(?is)<script[^>]*>.*?</script>", " ");
//                                    pageContent = pageContent.replaceAll("(?is)<style[^>]*>.*?</style>", " ");
//                                    pageContent = pageContent.replaceAll("(?i)<img[^>]*>", " ");
//                                    pageContent = pageContent.replaceAll("(?i)<video[^>]*>", " ");
                                    pageContent = processSpecificFields(pageContent);
                                    pageContent = pageContent.replaceAll("(?is)<[a-zA-Z]*script[a-zA-Z]*[^>]*>.*?</[a-zA-Z]*script[a-zA-Z]*>", " ");
                                    pageContent = pageContent.replaceAll("(?is)<[a-zA-Z]*style[a-zA-Z]*[^>]*>.*?</[a-zA-Z]*style[a-zA-Z]*>", " ");
                                    pageContent = pageContent.replaceAll("(?i)<[a-zA-Z]*img[a-zA-Z]*[^>]*>", " ");
                                    pageContent = pageContent.replaceAll("(?is)<[a-zA-Z]*video[a-zA-Z]*[^>]*>.*?</[a-zA-Z]*video[a-zA-Z]*>", " ");
                                    pageContent = pageContent.replaceAll("(?is)<[a-zA-Z]*svg[a-zA-Z]*[^>]*>.*?</[a-zA-Z]*svg[a-zA-Z]*>", " ");
                                } catch (Exception e) {
                                    e.getMessage();
                                }

                                double pageSize = pageContent.getBytes().length/(1024.0 * 1024.0);
                                System.out.println("Page Size: " + pageSize + " " + link);
                                if(pageSize > 0 && pageSize <= 2.0){
                                    row.put("page", pageContent);
                                }
//                                row.put("page", arr);
                                newUrls.addAll(normalizedUrls);
                            }


                        }
                    } else if (responseCode == 301 || responseCode == 302 || responseCode == 303 ||
                            responseCode == 307 || responseCode == 308) {
                        String newUrl = headConn.getHeaderField("Location");
//                        System.out.println("Redirected url: " + newUrl);
                        if (newUrl != null && !newUrl.isEmpty()) {
                            List<String> normalizedSeed = normalizeUrls(Arrays.asList(newUrl), url);
                            if (!normalizedSeed.isEmpty()) {
//                                System.out.println("Redirect then normalized url: " + normalizedSeed.get(0));
                                newUrls.add(normalizedSeed.get(0));
                            }
//                            newUrls.add(newUrl);
                            row.put("responseCode", String.valueOf(responseCode));
                            client.putRow("pt-crawl", row);
                            return newUrls;
                        }
                    } else {
                        row.put("responseCode", String.valueOf(responseCode));
                        client.putRow("pt-crawl", row);
                        return new ArrayList<>();
                    }

                    row.put("responseCode", String.valueOf(responseCode));
                    client.putRow("pt-crawl", row);
                    return newUrls;
                });
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

//                Thread.sleep(1000);

        }


    }

    public static List<String> extractURL(String content) {
        List<String> urls = new ArrayList<>();
        Pattern pattern = Pattern.compile("<a\\s+(?:[^>]*?\\s+)?href=\"([^\"]*)\"", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(content);
        while (matcher.find()) {
//            if(matcher.group(1) != null && !matcher.group(1).contains("wikimediafoundation.org") && !matcher.group(1).contains("mediawiki.org")){
            urls.add(matcher.group(1));
//            }
        }
        return urls;
    }


    public static List<String> normalizeUrls(List<String> extractedUrls, String baseUrl) {
        List<String> normalizedUrls = new ArrayList<>();
        String[] baseParts = URLParser.parseURL(baseUrl);

        for (String url : extractedUrls) {
            try {
//                System.out.println("url: " + url);
                String noFragmentUrl;
                String[] fragmentUrl = url.split("#");

                if (fragmentUrl == null || fragmentUrl.length == 0) {
                    continue;
                } else {
                    noFragmentUrl = fragmentUrl[0];
                }

                String[] urlParts = URLParser.parseURL(noFragmentUrl);

                if (urlParts[0] != null && !urlParts[0].equalsIgnoreCase("http") && !urlParts[0].equalsIgnoreCase("https")) {
                    continue;
                }
                if (url.endsWith(".jpg") || url.endsWith(".jpeg") || url.endsWith(".gif") || url.endsWith(".png") || url.endsWith(".txt")) {
                    continue;
                }

                String protocol;
                String host;
                boolean absolutePath = false;

                if (urlParts[3] == null) {
                    protocol = urlParts[0] != null ? urlParts[0] : "http";
                    host = urlParts[1] != null ? urlParts[1] : baseParts[1];
                    absolutePath = true;
                } else {
                    protocol = baseParts[0] != null ? baseParts[0] : "http";
                    host = urlParts[1] != null ? urlParts[1] : baseParts[1];
                }

                String defaultPort = "";
                if (protocol.equalsIgnoreCase("https")) {
                    defaultPort = ":443";
                } else if (protocol.equalsIgnoreCase("http")) {
                    defaultPort = ":80";
                }
                String port;

                if (urlParts[2] != null) {
                    port = ":" + urlParts[2];
                } else {
                    if (!absolutePath && baseParts[2] != null) {
                        port = ":" + baseParts[2];
                    } else {
                        port = defaultPort;
                    }
                }

                String resolvePath = urlParts[3].startsWith("/") ? urlParts[3] : baseParts[3] + "/" + urlParts[3];
                String path = urlParts[3] != null ? resolvePath : baseParts[3];

                if (!urlParts[3].startsWith("/") && baseParts[3] != null) {
                    String hostPath = null;
                    if (baseParts[3].lastIndexOf('/') > 0) {
                        hostPath = baseParts[3].substring(0, baseParts[3].lastIndexOf('/'));
                    }
                    String urlPath = urlParts[3].startsWith("/") ? urlParts[3] : "/" + urlParts[3];
                    resolvePath = hostPath == null ? urlPath : hostPath + urlPath;
                    path = urlParts[3] != null ? resolvePath : baseParts[3];
                }

                if (path.contains("..")) {
                    path = normalizePath(path);
                }

                String normalizedUrl = protocol + "://" + host + port + path;

                normalizedUrls.add(normalizedUrl);
            } catch (Exception e) {
                e.printStackTrace();
                e.getMessage();
            }
        }
        return normalizedUrls;
    }

    public static String normalizePath(String path) {
        String[] segments = path.split("/");
        LinkedList<String> resolvedSegments = new LinkedList<>();

        for (String segment : segments) {
            if ("..".equals(segment)) {
                if (!resolvedSegments.isEmpty()) {
                    resolvedSegments.removeLast();
                }
            } else if (!".".equals(segment) && !segment.isEmpty()) {
                resolvedSegments.add(segment);
            }
        }
        StringBuilder resolvedPath = new StringBuilder();
        for (String segment : resolvedSegments) {
            resolvedPath.append("/").append(segment);
        }

        return resolvedPath.toString();
    }


    public static long getLastAccessTime(KVSClient kvs, String host) {
        try {
            Row row = kvs.getRow("hosts", host);
            if (row != null) {
                String time = row.get("lastAccessTime");
                if (time != null) {
                    return Long.parseLong(time);
                }
            } else {
                System.out.println("Parsing new Host URL: " + host);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return 0;
    }

    public static void updatelastAccessTime(KVSClient kvs, String host, long lastAccessTime) {
        Row row = new Row(host);
        row.put("lastAccessTime", Long.toString(lastAccessTime));
        try {
            kvs.putRow("hosts", row);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean isUrlAllowed(KVSClient kvs, String host, String path) {
        if (!robotParsed.contains(host)) {
            try {
                URL robotsUrl = new URL("http://" + host + "/robots.txt");
                HttpURLConnection conn = (HttpURLConnection) robotsUrl.openConnection();
                conn.setRequestMethod("GET");
                conn.setRequestProperty("User-agent", "cis5550-crawler");
                if (conn.getResponseCode() == 200) {
                    try {
                        parseRobot(kvs, host, conn.getInputStream());
                    } catch (Exception e) {
                        e.getMessage();
                    }
                    robotParsed.add(host);
                }
            } catch (Exception e) {
                System.err.println("Error parsing robots.txt: " + host);
                robotParsed.add(host);
            }
        }
        return isPathAllowed(kvs, host, path);
    }

    public static void parseRobot(KVSClient kvs, String host, InputStream inputStream) throws IOException {
        System.out.println("Robot File Parsed");
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        String line;
        boolean userAgentMatch = false;
        String allow = null;
        String disallow = null;
        String delay = null;
        while ((line = reader.readLine()) != null) {
//            System.out.println("line: " + line);
            if (line.toLowerCase().startsWith("user-agent:")) {
                String userAgent = line.split(":")[1].trim();
                if (userAgent.toLowerCase().contains("cis5550-crawler") || userAgent.equals("*")) {
                    userAgentMatch = true;
                }
            } else if (userAgentMatch) {
                if (line.toLowerCase().startsWith("allow:")) {
                    allow = line.substring("allow:".length()).trim();
//                    System.out.println("allow: " + allow);
                } else if (line.toLowerCase().startsWith("disallow:")) {
                    disallow = line.substring("disallow:".length()).trim();
//                    System.out.println("disallow: " + disallow);
                } else if (line.toLowerCase().startsWith("crawl-delay:")) {
                    delay = line.substring("crawl-delay:".length()).trim();
//                    System.out.println("delay: " + delay);
                }
            }
        }
        Row row = new Row(host);
        if (allow != null) {
            row.put("allow", allow);
        }
        if (disallow != null) {
            row.put("disallow", disallow);
        }
        if (delay != null) {
            row.put("crawl-delay", delay);
        }

        kvs.putRow("robots", row);
    }

    private static boolean isPathAllowed(KVSClient kvs, String host, String urlPath) {
        try {
            Row row = kvs.getRow("robots", host);
//            System.out.println(row);
            if (row == null) {
                return true;
            }
            String allow = row.get("allow");
//            System.out.println("allow: " + allow);
            String disallow = row.get("disallow");
//            System.out.println("disallow: " + disallow);
            String delay = row.get("delay");
//            System.out.println("url check in isPathAllowed: " + urlPath);

//            System.out.println(disallow == null);

//            Allow: /abc followed by Disallow: /a would allow /abcdef and /xyz but forbid /alpha
//            Allow: / Disallow: /nocrawl
            if (disallow != null && allow != null) {
                if (disallow.length() >= allow.length()) {
                    if (urlPath.startsWith(disallow)) {
                        return false;
                    }
                } else {
                    if (urlPath.startsWith(allow)) {
                        return true;
                    }
                    if (urlPath.startsWith(disallow)) {
                        return false;
                    }
                }
            }
            if (allow != null) {
//                for (String allowedPath : allow.split(";")) {
                if (urlPath.startsWith(allow)) {
                    return true;
                }
//                }
            }

            if (disallow != null) {
//                System.out.println("disallow");
//                for (String disallowedPath : disallow.split(";")) {
                if (urlPath.startsWith(disallow)) {
//                        System.out.println("not allowed");
                    return false;
                }
//                }
            }


            return true;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void logQueueContents(FlameRDD urlQueue) {
        try {
            List<String> urls = urlQueue.collect();  // Assumes FlameRDD has a collect method
            logger.info("Queue Contents: " + urls);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String processSpecificFields(String htmlContent) {
        StringBuilder relevantText = new StringBuilder();

        addMatches(relevantText, htmlContent, "<title[^>]*>(.*?)</title>");

        // Extract headers
        addMatches(relevantText, htmlContent, "<h[1-6][^>]*>(.*?)</h[1-6]>");

        // Extract main body content
        addMatches(relevantText, htmlContent, "<p[^>]*>(.*?)</p>");


        // Process the extracted text
        return relevantText.toString();
    }

    private static void addMatches(StringBuilder relevantText, String htmlContent, String regex) {
        Pattern pattern = Pattern.compile(regex, Pattern.DOTALL);
        Matcher matcher = pattern.matcher(htmlContent);
        while (matcher.find()) {
            for (int i = 1; i <= matcher.groupCount(); i++) {
                if (matcher.group(i) != null) {
                    relevantText.append(matcher.group(i)).append(" ");
                }
            }
        }
    }



}
