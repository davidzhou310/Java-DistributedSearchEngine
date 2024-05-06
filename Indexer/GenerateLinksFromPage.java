package cis5550.jobs;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import cis5550.tools.Logger;
import cis5550.tools.URLParser;

public class GenerateLinksFromPage {
    private static final Logger logger = Logger.getLogger(cis5550.jobs.GenerateLinksFromPage.class);

    public static List<String> extractAndNormalizeURLs(String baseURL, String page) {
        List<String> result = new ArrayList<>();
        // Regex to match <a> tags and specifically capture href values regardless of the position of the href attribute
        String regex = "<a\\s+(?:[^>]*?\\s+)?href\\s*=\\s*(\"([^\"]*)\"|'([^']*)'|([^\\s>\"']+))(?:\\s+[^>]*)?>";
        Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(page);

        while (matcher.find()) {
            // The URL is captured in one of the three groups depending on the quote type
            String url = matcher.group(2) != null ? matcher.group(2) : (matcher.group(3) != null ? matcher.group(3) : matcher.group(4));

            if (url != null && !url.isEmpty()) {
                String urlNormalized = normalizeAndFilterURL(url, baseURL);
                if (urlNormalized != null && !urlNormalized.isEmpty()) {
                    result.add(urlNormalized);
                }
            }
        }
        return result;
        
    }
    
    // baseURL is the url for the current page
    private static String normalizeAndFilterURL(String url, String baseURL) {
     // first tokenize the baseURL
        String[] baseURLParts = URLParser.parseURL(baseURL);
        
        // use URLParser
        String[] urlParts = URLParser.parseURL(url);
        // store the result
        String normalizedURL = null;
        StringBuilder sb = new StringBuilder();
        
        if (urlParts[0] == null && urlParts[1] == null && urlParts[2] == null) {
//            logger.info("url: "+url + " base:" + baseURL);
            // no host part, only relative path
            // build the previous parts based on base url
            String baseProtoal = baseURLParts[0];
            String baseDomain = baseURLParts[1];
            String basePort = baseURLParts[2];
            // append the required protocal and domain name and port number
            sb.append(baseProtoal);
            sb.append("://");
            sb.append(baseDomain);
            sb.append(":");
            sb.append(basePort);
            
            // now consider the path part
            String urlPath = urlParts[3];
            
            if (urlPath.startsWith("/")) {
                // this url is relative to the domain of the current page
                // aka absolute but lack a host name(/abc/def.html)
                // append the relative url
                sb.append(urlPath);
            }else {
                // this url is relative to the url of the current page, 
                //(foo.html, bar/xyz.html, ../blah.html)
                // cut off the part in the base URL after the last /
                String baseURLPath = baseURLParts[3];
                
                // if baseURLPath is already "/", it becomes "", otherwise, it is /xxx
                String modifiedBaseURLPath = baseURLPath.substring(0, baseURLPath.lastIndexOf("/"));
                // and, for each .. in the link, delete the part to the previous /; then append the link.
                String[] urlPathTokens = urlPath.split("/");
                for (String urlPathToken: urlPathTokens) {
                    if(!urlPathToken.isEmpty()) {
                        if (urlPathToken.equals("..")) {
                            // delete the part to the previous /
                            modifiedBaseURLPath = handleDotDot(modifiedBaseURLPath);
                        }else {
                            // is words
                            // go down one level
                            modifiedBaseURLPath += "/" + urlPathToken;
                        }
                    }
                    // if is empty, just continue
                }
                sb.append(modifiedBaseURLPath);
            }
            normalizedURL = sb.toString();
        }else {
//            logger.info("else url: "+url + " base:" + baseURL);
            // it should have 0 and 1, but may not have 2
            sb.append(urlParts[0]);
            sb.append("://");
            sb.append(urlParts[1]);
            sb.append(":");
            String port = urlParts[2];
            // if it does have a host part but
            // not a port number, add the default port number for the protocol (http or https).
            if (port == null || port.equals("") || !url.contains(":")) {
                // use the default ports
                port = (urlParts[0].equals("http")) ? "80" : "443";///////////////////////////////////////////////////////////////
            }
            
            sb.append(port);
            sb.append(urlParts[3]);
            normalizedURL = sb.toString();
//            logger.info("else after normalize url: "+normalizedURL + " base:" + baseURL);
        }
        
        if (shouldFilterOut(normalizedURL)) {
            return null;
        }
        
        // check this at the end
        if(normalizedURL.indexOf("#") != -1) {
            // cut off the part after #
            normalizedURL = normalizedURL.substring(0, normalizedURL.indexOf("#"));
        }
        
        if (normalizedURL.isEmpty()) {
            // discard the url
            return null;
        }
        
        return normalizedURL;
    }
    
    private static String handleDotDot(String modifiedBaseURLPath) {
        if(modifiedBaseURLPath.equals("")) {
            return "";
        }
        return modifiedBaseURLPath.substring(0, modifiedBaseURLPath.lastIndexOf("/"));
    }
    
    private static boolean shouldFilterOut(String url) {
        if (!url.startsWith("http://") && !url.startsWith("https://")) {
            return true;
        }

        String[] ignoreExtensions = {".jpg", ".jpeg", ".gif", ".png", ".txt"};
        
        for (String extension : ignoreExtensions) {
            if (url.toLowerCase().endsWith(extension)) {
                return true;
            }
        }

        return false;
    }
}
