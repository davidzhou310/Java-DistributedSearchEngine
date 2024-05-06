package Webserver;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.*;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.*;
import javax.net.ServerSocketFactory;
import javax.net.ssl.*;
import java.security.*;

import static Webserver.ServerMethod.*;

public class Server {
    private static final long SESSION_CLEANUP_INTERVAL = 60 * 1000;
    protected static int HTTPPort = 80;
    protected static int HTTPSPort = 443;

    private String staticPath;
    private static final int NUM_WORKERS = 100;
    private final BlockingDeque<Socket> acceptQ;
    private final ExecutorService threadPool;
    private boolean serverOn;
    private ServerSocket HTTPServerSocket;
    private ServerSocket HTTPSServerSocket;
    private final ConcurrentHashMap<String, Route> getRouter;
    private final ConcurrentHashMap<String, Route> postRouter;
    private final ConcurrentHashMap<String, Route> putRouter;

    public Server() {
        this.acceptQ = new LinkedBlockingDeque<>();
        this.threadPool = Executors.newFixedThreadPool(NUM_WORKERS);
        this.serverOn = false;
        this.HTTPServerSocket = null;
        this.HTTPSServerSocket = null;
        this.getRouter = new ConcurrentHashMap<>();
        this.postRouter = new ConcurrentHashMap<>();
        this.putRouter = new ConcurrentHashMap<>();

        ClassLoader classLoader = getClass().getClassLoader();
        URL url = classLoader.getResource("");
        if (url == null) { throw new RuntimeException("Failed to start server"); }
        this.staticPath = url.getPath();
    }


    public void staticFiles(String path) {
        if (path.startsWith("/")) {
            staticPath += path.substring(1);
            return;
        } else if (path.startsWith("./")) {
            staticPath += path.substring(2);
            return;
        }

        String[] staticParts = path.split("/");
        String[] loaderParts = staticPath.toString().split("/");

        if (staticParts.length == 1) {
            staticPath += path;
            return;
        }

        int prevCount = (int) Arrays.stream(staticParts).filter(parts -> parts.equals("..")).count();
        StringBuilder newPath = new StringBuilder();

        for (int i = 0; i < loaderParts.length - prevCount; i++) {
            newPath.append(loaderParts[i]).append("/");
        }

        for (int i = prevCount; i < staticParts.length; i++) {
            newPath.append(staticParts[i]).append("/");
        }

        staticPath = newPath.toString();
    }

    public void port(int p) { HTTPPort = p; }

    public void securePort(int p) { HTTPSPort = p; }

    public void useSecurePort() throws Exception {
        String pwd = "secret";
        KeyStore keyStore = KeyStore.getInstance("JKS");
        keyStore.load(new FileInputStream("keystore.jks"), pwd.toCharArray());
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
        keyManagerFactory.init(keyStore, pwd.toCharArray());
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(keyManagerFactory.getKeyManagers(), null, null);
        ServerSocketFactory factory = sslContext.getServerSocketFactory();
        HTTPSServerSocket = factory.createServerSocket(HTTPSPort);
    }

    private void acceptConnection(ServerSocket serverSocket) throws Exception {
        while (true) {
            Socket s = serverSocket.accept();
            acceptQ.put(s);
        }
    }

    public Route getRouter(Request req) {
        return switch (req.requestMethod()) {
            case "GET", "HEADER" -> routerMatcher(getRouter, req);
            case "POST" -> routerMatcher(postRouter, req);
            case "PUT" -> routerMatcher(putRouter, req);
            default -> null;
        };
    }

    private void processRequest(Socket s) throws IOException {
        boolean closeFlag = false;
        while (!closeFlag) {
            try {
                ArrayList<Byte> buffer = new ArrayList<>();
                while (!checkFullHeader(buffer)) {
                    int b = s.getInputStream().read();
                    if (b == -1) {
                        closeFlag = true;
                        break;
                    }
                    buffer.add((byte) b);
                }
                if (closeFlag) {
                    break;
                }


                byte[] header = new byte[buffer.size()];
                for (int i = 0; i < buffer.size(); i++) {
                    header[i] = buffer.get(i);
                }
                Request req = pareseRequest(new String(header), (InetSocketAddress) s.getRemoteSocketAddress());
                req.parseQueryParams();

                if (req.contentLength() != -1) {
                    byte[] body = new byte[req.contentLength()];
                    int i = s.getInputStream().read(body);
                    if (i == -1) {
                        closeFlag = true;
                    }
                    req.appendBody(body);
                }
                Route router = getRouter(req);
                Response res = new ResponseImpl(s);
                Object o = null;
                if (router == null) {
                    generateStaticResponse(req, res, staticPath);
                } else {
                    try {
                        o = router.handle(req, res);
                        Session session = req.getSession();
                        if (session != null && session.isNewSession()) {
                            session.existedSession();
                            res.header("Set-Cookie", "SessionID=" + session.id());
                        }
                    } catch (Exception e) {
                        res.status(500, "Internal Server Error");
                    }
                }
                if (res.getWriteCalled() == 0) {
                    sendResponse(s, res.generateResponse(o));
                }
            } catch (Exception e) {
                e.printStackTrace();
                break;
            }
        }
        s.close();
    }

    private void checkServer() {
        if (!serverOn) {
            serverOn = true;
            threadPool.execute(() -> {
                try {
                    HTTPServerSocket = new ServerSocket(HTTPPort);
                    acceptConnection(HTTPServerSocket);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            threadPool.execute(() -> {
                try {
                    if (HTTPSServerSocket != null ) { acceptConnection(HTTPSServerSocket); }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            threadPool.execute(() -> {
                while (true) {
                    try {
                        Thread.sleep(SESSION_CLEANUP_INTERVAL);
                        SessionManager.cleanupExpiredSessions();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
            for (int i = 0; i < NUM_WORKERS - 3; i++) {
                threadPool.execute(this::run);
            }
        }
    }

    public void run() {
        while (true) {
            try {
                Socket s = acceptQ.take();
                try {
                    processRequest(s);
                } catch (IOException e) {
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void get(String path, Route route) {
        checkServer();
        getRouter.put(path, route);
    }

    public void post(String path, Route route) {
        checkServer();
        postRouter.put(path, route);
    }

    public void put(String path, Route route) {
        checkServer();
        putRouter.put(path, route);
    }
}
