package KVS.generic;

import java.io.*;
import java.net.URL;
import java.util.*;

public class Worker {
    protected static final String WORKER_DIR = "pt-tables";
    protected static void startPingThread(String address, String id, int port) {
        (new Thread("Ping Thread") {
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(5000);
                        URL url = new URL("http://" + address + "/ping?id=" + id + "&port=" + port);
                        url.getContent();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
    }

    protected static String[] getPermTables() {
        File workerDir = new File(WORKER_DIR);
        if (!workerDir.exists()) {
            return new String[0];
        }
        TreeSet<String> permTables = new TreeSet<>();
        for (String file : Objects.requireNonNull(workerDir.list())) {
            if (file.startsWith("pt-")) {
                permTables.add(file);
            }
        }
        return permTables.toArray(new String[0]);
    }

    protected static String generateRandomId() {
        StringBuilder id = new StringBuilder();
        for (int i = 0; i < 5; i++) {
            Random random = new Random();
            id.append((char) (random.nextInt(26) + 'a'));
        }
        return id.toString();
    }
}