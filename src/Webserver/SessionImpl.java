package Webserver;

import java.util.HashMap;

public class SessionImpl implements Session {
    private boolean valid = true;
    private final String id;
    private final long creationTime;
    private long lastAccessedTime;
    private int maxActiveInterval = 300;
    private boolean newSession;
    private final HashMap<String, Object> attributes = new HashMap<>();

    public SessionImpl (String id) {
        this.id = id;
        creationTime = System.currentTimeMillis();
        lastAccessedTime = System.currentTimeMillis();
        newSession = true;
    }

    public String id() { return id; }
    public boolean getValidity() { return valid; }
    public void invalid() { valid = false; }
    public long creationTime() { return creationTime; }
    public void setLastAccessedTime(long time) { lastAccessedTime = time; }
    public long lastAccessedTime() { return lastAccessedTime; }
    public void maxActiveInterval(int seconds) { maxActiveInterval = seconds; }
    public int getMaxActiveInterval() { return maxActiveInterval; }
    public void invalidate() { valid = false; }
    public void existedSession() { newSession = false; }
    public boolean isNewSession() { return newSession; }
    public Object attribute(String name) { return attributes.getOrDefault(name, null); }
    public void attribute(String name, Object value) { attributes.put(name, value); }
}
