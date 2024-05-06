import Webserver.Server;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

public class StartServer {
    private static final String HOME_PAGE_PATH = "static/template/HomePage.html";
    private static final String RESULT_PAGE_PATH = "static/template/PageResults.html";
    private static final int LIMIT = 15;
    private static String loadPage(String path) {
        File Page = new File(path);
        if (!Page.exists()) {
            return "404 Not Found";
        }
        try {
            FileInputStream in = new FileInputStream(Page);

            byte[] buffer = in.readAllBytes();
            in.close();

            return new String(buffer);
        } catch (IOException e) {
            return "500 Internal Server Error";
        }
    }

    public static void main(String[] args) {
        Server server = new Server();
        server.port(9000);
        server.securePort(443);
        server.staticFiles("../static");

        server.get("/", (req, res) -> {
            String template = loadPage(HOME_PAGE_PATH);
            if (template.equals("404 Not Found")) {
                res.status(404, "Not Found");
            } else if (template.equals("500 Internal Server Error")) {
                res.status(500, "Internal Server Error");
            } else {
                res.status(200, "OK");
                res.header("Content-Type", "text/html");
            }

            return template;
        });

        server.get("/result", (req, res) -> {
            String template = loadPage(RESULT_PAGE_PATH);
            if (template.equals("404 Not Found")) {
                res.status(404, "Not Found");
            } else if (template.equals("500 Internal Server Error")) {
                res.status(500, "Internal Server Error");
            } else {
                res.status(200, "OK");
                res.header("Content-Type", "text/html");
            }

            return template;
        });

        server.get("/search", (req, res) -> {
            String query = req.queryParams("query");
            int page = Integer.parseInt(req.queryParams("page"));
            List<String> urls = ProcessQuery.getResults("18.210.17.43:8000", query.split(" "));

            if ((page - 1) * LIMIT >= urls.size()) {
                res.type("application/json");
                res.status(200, "OK");
                return "[]";
            }
            
            List<String> pageURLs = urls.subList((page - 1) * LIMIT, Math.min(page * LIMIT, urls.size()));
            StringBuilder result = new StringBuilder("[");

            pageURLs.forEach(el -> {
                result.append("{\"url\": \"").append(el).append("\"").append("},");
            });
            result.deleteCharAt(result.length() - 1);
            result.append("]");

            res.type("application/json");
            res.status(200, "OK");

            return result.toString();
        });
    }
}