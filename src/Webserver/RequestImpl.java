package Webserver;

import java.util.*;
import java.net.*;
import java.nio.charset.*;

// Provided as part of the framework code

class RequestImpl implements Request {
    String method;
    String url;
    String protocol;
    InetSocketAddress remoteAddr;
    Map<String,String> headers;
    Map<String,String> queryParams;
    Map<String,String> params;
    byte[] bodyRaw;
    Session session;

    RequestImpl(String methodArg, String urlArg, String protocolArg, InetSocketAddress remoteAddrArg) {
        method = methodArg;
        url = urlArg;
        protocol = protocolArg;
        headers = new HashMap<>();
        queryParams = new HashMap<>();
        remoteAddr = remoteAddrArg;
    }

    public String requestMethod() {
        return method;
    }
    public void setParams(Map<String,String> paramsArg) {
        params = paramsArg;
    }
    public int port() {
        return remoteAddr.getPort();
    }
    public String url() {
        return url;
    }
    public String protocol() {
        return protocol;
    }
    public String contentType() { return headers.get("content-type"); }
    public String ip() {
        return remoteAddr.getAddress().getHostAddress();
    }
    public String body() {
        return new String(bodyRaw, StandardCharsets.UTF_8);
    }
    public byte[] bodyAsBytes() {
        return bodyRaw;
    }
    public int contentLength() {
        return Integer.parseInt(headers.getOrDefault("content-length", "-1"));
    }
    public String headers(String name) {
        return headers.get(name.toLowerCase());
    }
    public Set<String> headers() {
        return headers.keySet();
    }
    public String queryParams(String param) {
        return queryParams.get(param);
    }
    public Set<String> queryParams() {
        return queryParams.keySet();
    }
    public String params(String param) {
        return params.get(param);
    }
    public Map<String,String> params() {
        return params;
    }
    public Session getSession() { return session; }
    public void setHeaderParams(String key, String value) {
        headers.put(key, value);
    }
    public void appendBody(byte[] buffer) {
        if (bodyRaw == null) {
            bodyRaw = buffer;
        } else {
            byte[] newBody = new byte[bodyRaw.length + buffer.length];
            System.arraycopy(bodyRaw, 0, newBody, 0, bodyRaw.length);
            System.arraycopy(buffer, 0, newBody, bodyRaw.length, buffer.length);
            bodyRaw = newBody;
        }
    }
    public byte[] getRowBody() { return bodyRaw; }

    public void parseQueryParams() {
        if (url.contains("?")) {
            String queryPart = url.split("\\?")[1];
            String[] queryPairs = queryPart.split("&");
            for (String pair : queryPairs) {
                String[] keyValue = pair.split("=");
                if (keyValue.length == 1) {
                    queryParams.put(URLDecoder.decode(keyValue[0], StandardCharsets.UTF_8), "");
                } else {
                    queryParams.put(URLDecoder.decode(keyValue[0], StandardCharsets.UTF_8), URLDecoder.decode(keyValue[1], StandardCharsets.UTF_8));
                }
            }
        }
        if (Objects.equals(contentType(), "application/x-www-form-urlencoded") && body() != null) {
            String[] queryPairs = body().split("&");
            for (String pair : queryPairs) {
                String[] keyValue = pair.split("=");
                queryParams.put(URLDecoder.decode(keyValue[0], StandardCharsets.UTF_8), URLDecoder.decode(keyValue[1], StandardCharsets.UTF_8));
            }
        }
    }

    public Session session() {
        if (session != null && !SessionManager.checkSessionExpiry(session)) { return session; }
        if (headers("cookie") != null) {
            String[] cookies = headers("cookie").split("; ");
            for (String cookie : cookies) {
                String[] keyValue = cookie.split("=");
                if (keyValue[0].equals("SessionID")) {
                    Session s =  SessionManager.getSession(keyValue[1]);
                    if (s != null) {
                        session = s;
                        return s;
                    } else {
                        session = SessionManager.createNewSession();
                        return session;
                    }
                }
            }
        }
        session = SessionManager.createNewSession();
        return session;
    }
}

