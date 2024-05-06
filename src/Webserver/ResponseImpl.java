package Webserver;

import java.net.Socket;
import java.util.HashMap;

class ResponseImpl implements Response {
    byte[] body;
    String reasonPhrase = "OK";
    int statusCode = 200;
    HashMap<String, String> header;
    String protocol = "HTTP/1.1";
    String serverName = "MyServer";
    Socket s;
    int writeCalled = 0;

    public ResponseImpl(Socket s) {
        this.header = new HashMap<>();
        type("text/plain");
        this.s = s;
    }

    public void body(String body) {
        this.body = body.getBytes();
        header("Content-Length", String.valueOf(this.body.length));
    }

    public void bodyAsBytes(byte[] bodyArg) {
        this.body = bodyArg;
        header("Content-Length", String.valueOf(this.body.length));
    }

    public void header(String name, String value) { this.header.put(name, value); }

    public void type(String contentType) { this.header.put("Content-Type", contentType); }

    public void status(int statusCode, String reasonPhrase) {
        this.statusCode = statusCode;
        this.reasonPhrase = reasonPhrase;
    }

    public byte[] generateResponse(Object o) {
        if (o != null) {
            this.body(o.toString());
        }
        String header = this.protocol + " " + this.statusCode + " " + this.reasonPhrase + "\r\n" +
                "Server: " + this.serverName + "\r\n";
        if (this.header != null) {
            for (String key : this.header.keySet()) {
                header = header.concat(key + ": " + this.header.get(key) + "\r\n");
            }
        }
        byte[] ByteHeader = (header.concat("\r\n")).getBytes();
        if (this.body != null) {
            byte[] response = new byte[ByteHeader.length + this.body.length];
            System.arraycopy(ByteHeader, 0, response, 0, ByteHeader.length);
            System.arraycopy(this.body, 0, response, ByteHeader.length, this.body.length);
            return response;
        } else {
            return ByteHeader;
        }

    }

    public void removeHeader(String key) {
        if (this.header.get(key) != null) {
            this.header.remove(key);
        }
    }

    public void write(byte[] b) throws Exception {
        this.writeCalled += 1;

        if (this.writeCalled == 1) {
            this.bodyAsBytes(b);
            this.removeHeader("Content-Length");
            byte[] response = this.generateResponse(null);
            ServerMethod.sendResponse(this.s, response);
        } else if (this.writeCalled > 1) {
            ServerMethod.sendResponse(this.s, b);
        }
    }

    public int getWriteCalled() { return this.writeCalled; }

    public void redirect(String url, int responseCode) {
        switch (responseCode) {
            case 301:
                status(301, "Moved Permanently");
                for (String key: header.keySet()) {
                    removeHeader(key);
                }
                header("Location", url);
                bodyAsBytes(null);
                break;
            case 302:
                status(302, "Found");
                header("Location", url);
                break;
            case 303:
                status(303, "See Other");
                for (String key: header.keySet()) {
                    removeHeader(key);
                }
                header("Location", url);
                bodyAsBytes(null);
                break;
            case 307:
                status(307, "Temporary Redirect");
                for (String key: header.keySet()) {
                    removeHeader(key);
                }
                header("Location", url);
                bodyAsBytes(null);
                break;
            case 308:
                status(308, "Permanent Redirect");
                header("Location", url);
                break;
            default:
                break;
        }
    }
    public void halt(int statusCode, String reasonPhrase) {
        return;
    }
}
