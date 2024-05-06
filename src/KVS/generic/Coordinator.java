package KVS.generic;

import Webserver.Server;

import java.util.List;
import java.util.Vector;

public class Coordinator {
    private static final long INVALID_TIME = 1500000000;
    private static final Vector<WorkerInfo> allWorkers = new Vector<>();
    public static Vector<String> getWorkers(){
        checkInvalid();
        List<String> workers = allWorkers.stream().map(worker -> worker.getIp() + ":" + worker.getPort()).toList();
        return new Vector<>(workers);

    }

    public static String workerTable() {
        checkInvalid();
        StringBuilder table = new StringBuilder("<html><table border=\"1\"><thead>All Workers</thead>");

        for (WorkerInfo worker: allWorkers) {
            table.append("<tr><td><a href=\"http://").append(worker.getIp()).append(":").append(worker.getPort()).append("/\">");
            table.append(worker.getId());
            table.append("</a></td></tr>");
        }
        table.append("</table></html>");

        return table.toString();
    }

    private static void checkInvalid() {
        allWorkers.removeIf(worker -> System.currentTimeMillis() - worker.getLastPing() > INVALID_TIME);
    }

    private static int getWorkerIdx(String id) {
        checkInvalid();
        for (int i = 0; i < allWorkers.size(); i++) {
            if (allWorkers.get(i).getId().equals(id)) {
                return i;
            }
        }
        return -1;
    }

    public static void registerRoute(Server server) {
        server.get("/ping", (req, res) -> {
            if (req.queryParams("id") == null || req.queryParams("port") == null) {
                res.status(400, "Bad Request");
                return "Bad Request";
            }

            String id = req.queryParams("id");
            int port = Integer.parseInt(req.queryParams("port"));
            int idx = getWorkerIdx(id);
            if (idx < 0) {
                WorkerInfo worker = new WorkerInfo(req.ip(), port, id);
                allWorkers.add(worker);
            } else {
                WorkerInfo worker = allWorkers.get(idx);
                worker.setIp(req.ip());
                worker.setPort(port);
                worker.updateLastPing();
                allWorkers.set(idx, worker);
            }
            res.status(200, "OK");
            return "OK";
        });

        server.get("/workers", (req, res) -> {
            checkInvalid();
            StringBuilder workerString = new StringBuilder(Integer.toString(allWorkers.size())).append('\n');
            for (WorkerInfo worker: allWorkers) {
                workerString.append(worker.getId()).append(",").append(worker.getIp()).append(":").append(worker.getPort()).append("\n");
            }
            res.status(200, "OK");
            return workerString.toString();
        });

        server.get("/", (req, res) -> {
            res.status(200, "OK");
            res.type("text/html");
            return workerTable();
        });
    }
}

