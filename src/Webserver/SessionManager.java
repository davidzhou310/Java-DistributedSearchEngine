package Webserver;

import java.util.HashMap;

public class SessionManager {
    public static final HashMap<String, Session> sessionStorage = new HashMap<>();

    public static Session getSession(String sessionID) {
        Session s = sessionStorage.getOrDefault(sessionID, null);
        if (s != null && !checkSessionExpiry(s)) {
            s.setLastAccessedTime(System.currentTimeMillis());
            return s;
        }
        return null;
    }
    public static void addSession(String sessionID, Session session) {
        sessionStorage.put(sessionID, session);
    }
    public static Session createNewSession() {
        String sessionID = ServerMethod.generateRandomString(20);
        Session session = new SessionImpl(sessionID);
        addSession(sessionID, session);
        return session;
    }
    public static boolean checkSessionExpiry(Session session) {
        if (!session.getValidity()) { return true; }
        long currentTime = System.currentTimeMillis();
        if (session.lastAccessedTime() + (long) session.getMaxActiveInterval() * 1000 < currentTime) {
            session.invalid();
            return true;
        }
        return false;
    }
    public static void cleanupExpiredSessions() {
        for (String sessionID : sessionStorage.keySet()) {
            Session session = sessionStorage.get(sessionID);
            if (checkSessionExpiry(session)) {
                sessionStorage.remove(sessionID);
            }
        }
    }
}
