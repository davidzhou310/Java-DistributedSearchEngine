package Webserver;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class ServerMethod {

    private static String getContentType(String url) {
        if (url.endsWith(".html")) return "text/html";
        else if (url.endsWith(".jpg") || url.endsWith(".jpeg")) return "image/jpeg";
        else if (url.endsWith(".png")) return "image/png";
        else if (url.endsWith(".gif")) return "image/gif";
        else if (url.endsWith(".css")) return "text/css";
        else if (url.endsWith(".js")) return "text/javascript";
        else if (url.endsWith(".txt")) return "text/plain";
        return "application/octet-stream";
    }

    public static Request pareseRequest(String header, InetSocketAddress remoteAddress) {
        String[] lines = header.split("\r\n");
        String[] firstLine = lines[0].split(" ");
        if (firstLine.length != 3) return new RequestImpl("", "", "", remoteAddress);

        Request req = new RequestImpl(firstLine[0], firstLine[1], firstLine[2], remoteAddress);

        for (int i = 1; i < lines.length; i++) {
            String[] line = lines[i].split(": ");
            if (line.length == 2) {
                req.setHeaderParams(line[0].toLowerCase(), line[1]);
            }
        }

        return req;
    }

    public static void generateStaticResponse(Request req, Response res, String staticPath) {
        if (req.url().isEmpty()) return;
        try {
            File file = new File(staticPath + req.url());
            FileInputStream fis = new FileInputStream(file);
            res.bodyAsBytes(fis.readAllBytes());
            res.status(200, "OK");
            res.type(getContentType(req.url()));

        } catch (FileNotFoundException e) {
            res.status(404, "Not Found");
            res.type("text/plain");
            res.body("404 Not Found");
        } catch (Exception e) {
            res.status(403, "Forbidden");
            res.type("text/plain");
            res.body("403 Forbidden");
        }
    }

    public static String generateRandomString(int length) {
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder sb = new StringBuilder();
        SecureRandom random = new SecureRandom();
        for (int i = 0; i < length; i++) {
            int index = random.nextInt(chars.length());
            sb.append(chars.charAt(index));
        }

        return sb.toString();
    }

    public static void sendResponse(Socket s, byte[] response) throws Exception {
        OutputStream out = s.getOutputStream();
        out.write(response);
    }

    public static boolean checkFullHeader(ArrayList<Byte> buffer) {
        for (int i = 0; i < buffer.size() - 3; i++) {
            if (buffer.get(i) == '\r' && buffer.get(i + 1) == '\n' && buffer.get(i + 2) == '\r' && buffer.get(i + 3) == '\n') {
                return true;
            }
        }
        return false;
    }

    public static Route routerMatcher (ConcurrentHashMap<String, Route> router, Request req) {
        for (String path: router.keySet()) {
            if (path.equals(req.url())) return router.get(path);
            String[] pathArray = path.split("/");
            String[] urlArray = req.url().split("\\?")[0].split("/");
            if (pathArray.length != urlArray.length) continue;
            HashMap<String, String> params = new HashMap<>();
            int checked = 0;
            for (int i = 0; i < pathArray.length; i++) {
                if (!pathArray[i].equals(urlArray[i])) {
                    if (pathArray[i].charAt(0) == ':') {
                        params.put(pathArray[i].substring(1), URLDecoder.decode(urlArray[i], StandardCharsets.UTF_8));
                        checked++;
                    } else {
                        params.clear();
                        break;
                    }
                } else {
                    checked++;
                }
                if (checked == pathArray.length) {
                    req.setParams(params);
                    return router.get(path);
                }
            }
        }
        return null;
    }
}

