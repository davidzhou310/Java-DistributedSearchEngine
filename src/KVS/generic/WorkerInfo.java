package KVS.generic;

public class WorkerInfo {
    private String ip;
    private int port;
    private final String id;
    private long lastPing;

    public WorkerInfo(String ip, int port, String id) {
        this.ip = ip;
        this.port = port;
        this.id = id;
        this.lastPing = System.currentTimeMillis();
    }

    public String getIp() { return ip; }
    public void setIp(String ip) { this.ip = ip; }

    public int getPort() { return port; }

    public void setPort(int port) { this.port = port; }

    public String getId() { return id; }
    public void updateLastPing() { lastPing = System.currentTimeMillis(); }
    public long getLastPing() { return lastPing; }

}
