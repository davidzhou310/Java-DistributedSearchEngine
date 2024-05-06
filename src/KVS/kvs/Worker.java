package KVS.kvs;

import KVS.generic.Table;
import Webserver.Server;

import java.io.*;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Objects;

import static KVS.tools.KeyEncoder.encode;


public class Worker extends KVS.generic.Worker {
    private static final HashMap<String, Table> tables = new HashMap<>();

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(args[0]);
        String directory = args[1];
        String Address = args[2];

        File dir = new File(WORKER_DIR);
        if (!dir.exists()) {dir.mkdir();}

        Server server = new Server();

        server.port(port);

        if (!new File(directory).exists()) {new File(directory).mkdir();}

        File file = new File(directory + File.separator + "id");
        String id;
        if (!file.exists()) {
            BufferedWriter writer = new BufferedWriter(new FileWriter(file));
            id = generateRandomId();
            writer.write(id);
            writer.close();
        }
        BufferedReader reader = new BufferedReader(new FileReader(file));
        while ((id = reader.readLine()) != null) {
            startPingThread(Address, id, port);
        }
        reader.close();

        server.put("/data/:table", (req, res) -> {
            String tableName = req.params("table");
            Row row = Row.readFrom(new ByteArrayInputStream(req.bodyAsBytes()));
            if (row == null) {
                res.status(500, "Internal Server Error");
                return "bad row";
            }

            if (tableName.startsWith("pt-")) {
                File permTable = new File(WORKER_DIR + File.separator + tableName);
                if (!permTable.exists()) { permTable.mkdir(); }
                File newRow = new File(WORKER_DIR + File.separator + tableName + File.separator + encode(row.key()));
                FileOutputStream outputStream = new FileOutputStream(newRow);
                outputStream.write(row.toByteArray());
                outputStream.close();
            } else {
                if (tables.get(tableName) == null) {
                    Table table = new Table(tableName);
                    table.insertRow(row);
                    tables.put(tableName, table);
                } else {
                    tables.get(tableName).insertRow(row);
                }
            }
            res.status(200, "OK");
            return "OK";
        });

        server.put("/data/:table/:row/:col", (req, res) -> {
            String tableName = req.params("table");
            String rowName = req.params("row");
            String colName = req.params("col");
            byte[] data = req.bodyAsBytes();

            Row row = new Row(rowName);

            if (tableName.startsWith("pt-")) {
                File permTable = new File(WORKER_DIR + File.separator + tableName);
                if (!permTable.exists()) { permTable.mkdir(); }
                try {
                    File rowFile = new File(WORKER_DIR + File.separator + tableName + File.separator + encode(rowName));
                    if (rowFile.exists()) {
                        ByteArrayInputStream inputStream = new ByteArrayInputStream(Files.readAllBytes(rowFile.toPath()));
                        row = Row.readFrom(inputStream);
                        if (row == null) {
                            res.status(500, "Internal Server Error");
                            return "Bad reading from file";
                        }
                    }
                    row.put(colName, data);
                    FileOutputStream outputStream = new FileOutputStream(rowFile);
                    outputStream.write(row.toByteArray());
                    outputStream.close();
                } catch (Exception e) {
                    e.printStackTrace();
                    res.status(500, "Internal Server Error");
                    return "Internal Server Error";
                }
                res.status(200, "OK");
                return "OK";
            }
            if (tables.get(tableName) == null) {
                row.put(colName, data);
                Table table = new Table(tableName);
                table.insertRow(row);
                tables.put(tableName, table);
            } else if (tables.get(tableName).getRow(rowName) == null) {
                row.put(colName, data);
                tables.get(tableName).insertRow(row);
            } else {
                if (req.queryParams("ifcolumn") != null && req.queryParams("equals") != null) {
                    String ifColumn = req.queryParams("ifcolumn");
                    String equals = req.queryParams("equals");

                    if (tables.get(tableName).getRow(rowName).get(ifColumn).equals(equals)) {
                        res.status(200, "OK");
                        return "FAIL";
                    }
                }
                tables.get(tableName).columns.add(colName);
                tables.get(tableName).getRow(rowName).put(colName, data);
            }
            res.status(200, "OK");
            return "OK";
        });

        server.get("/data/:table/:row/:col", (req, res) -> {
            String tableName = req.params("table");
            String rowName = req.params("row");
            String colName = req.params("col");

            if (tableName.startsWith("pt-")) {
                File permTable = new File(WORKER_DIR + File.separator + tableName);
                if (!permTable.exists()) {
                    res.status(404, "Not Found");
                    return "Not Found";
                }
                File rowFile = new File(WORKER_DIR + File.separator + tableName + File.separator + encode(rowName));
                if (!rowFile.exists()) {
                    res.status(404, "Not Found");
                    return "Not Found";
                }
                ByteArrayInputStream inputStream = new ByteArrayInputStream(Files.readAllBytes(rowFile.toPath()));
                Row row = Row.readFrom(inputStream);
                if (row == null) {
                    res.status(500, "Internal Server Error");
                    return "Bad reading from file";
                }

                for (String column : row.columns()) {
                    if (column.equals(colName)) {
                        res.body(row.get(column));
                        res.status(200, "OK");
                        return null;
                    }
                }
                res.status(404, "Not Found");
                return "Not Found";
            }

            if (tables.get(tableName) == null) {
                res.status(404, "Not Found");
                return "Not Found";
            }

            Row row = tables.get(tableName).getRow(rowName);
            if (row == null) {
                res.status(404, "Not Found");
                return "Not Found";
            }

            if (!row.columns().contains(colName)) {
                res.status(404, "Not Found");
                return "Not Found";
            }

            byte[] data = row.getBytes(colName);

            res.bodyAsBytes(data);
            res.status(200, "OK");
            return null;

        });

        server.get("/", (req, res) -> {
            StringBuilder allTables = new StringBuilder("<html><table border=\"1\">");
            allTables.append("<thead><tr><th>Table</th><th>Number of Columns</th></tr></thead>");
            allTables.append("<tbody>");
            for (String table : tables.keySet()) {
                allTables.append("<tr><th><a href=\"http://").append(req.ip()).append(":").append(port);
                allTables.append("/view/").append(table).append("\">");
                allTables.append(table);
                allTables.append("</a></th>");
                allTables.append("<td>").append(tables.get(table).columns.size()).append("</td>");
                allTables.append("</tr>");
            }
            for (String table : getPermTables()) {
                allTables.append("<tr><th><a href=\"http://").append(req.ip()).append(":").append(port);
                allTables.append("/view/").append(table).append("\">");
                allTables.append(table);
                allTables.append("</a></th>");
                allTables.append("<td>").append(Objects.requireNonNull(new File(WORKER_DIR + File.separator + table).list()).length).append("</td>");
                allTables.append("</tr>");
            }
            allTables.append("</tbody>");
            allTables.append("</table></html>");
            res.type("text/html");
            res.status(200, "OK");
            res.body(allTables.toString());
            return null;
        });

        server.get("/tables", (req, res)-> {
            StringBuilder allTables = new StringBuilder();

            for (String table : tables.keySet()) { allTables.append(table).append("\n"); }
            for (String table : getPermTables()) { allTables.append(table).append("\n"); }

            res.type("text/plain");
            res.status(200, "OK");
            res.body(allTables.toString());
            return null;
        });

        server.get("/view/:table", (req, res) -> {
            String tableName = req.params("table");
            int page = req.queryParams("page") == null ? 1 : Integer.parseInt(req.queryParams("page"));
            String from = req.queryParams("fromRow") == null ? null : req.queryParams("fromRow");

            Table table = tables.get(tableName);

            if (tableName.startsWith("pt-")) {
                File permTable = new File(WORKER_DIR + File.separator + tableName);
                if (!permTable.exists()) {
                    res.status(404, "Not Found");
                    return "Not Found";
                }
                table = new Table(tableName);
                for (String rowKey : Objects.requireNonNull(permTable.list())) {
                    File rowFile = new File(WORKER_DIR + File.separator + tableName + File.separator + rowKey);
                    ByteArrayInputStream inputStream = new ByteArrayInputStream(Files.readAllBytes(rowFile.toPath()));
                    Row row = Row.readFrom(inputStream);
                    if (row == null) {
                        res.status(500, "Internal Server Error");
                        return "Bad reading from file";
                    }

                    table.insertRow(row);
                }
            }

            StringBuilder content = new StringBuilder("<html><table border=\"1\"><caption>");
            content.append(tableName);
            content.append("</caption><thread>");
            content.append("<tr><th>RowKey</th>");
            for (String column : table.columns) {
                content.append("<th>").append(column).append("</th>");
            }
            content.append("</tr></thread>");

            String[] keys = table.sortedRows();
            if (from != null) {
                int i = 0;
                while (i < keys.length && keys[i].compareTo(from) < 0) { i++; }
                keys = Arrays.copyOfRange(keys, i, keys.length);
            }

            for (int i = (page - 1) * 10; i < Math.min(page * 10, keys.length); i++) {
                content.append("<tr><td>");
                content.append(keys[i]);
                content.append("</td>");
                for (String column : table.columns) {
                    content.append("<td>");
                    content.append(table.getRow(keys[i]).get(column));
                    content.append("</td>");
                }
                content.append("</tr>");
            }
            content.append("</table></html>");

            if (page * 10 < keys.length) {
                content.append("<div><a href=\"http://").append(req.ip()).append(":").append(port);
                content.append("/view/").append(tableName).append("?page=").append(page + 1).append("\">");
                content.append("Next Page");
                content.append("</a></div>");
            }

            res.type("text/html");
            res.status(200, "OK");
            res.body(content.toString());
            return null;
        });

        server.get("/data/:table/:row", (req, res) -> {
            String tableName = req.params("table");
            String rowName = req.params("row");
            Row row;

            if (tableName.startsWith("pt-")) {
                File permTable = new File(WORKER_DIR + File.separator + tableName);
                if (!permTable.exists()) {
                    res.status(404, "Not Found");
                    return "Not Found";
                }

                File rowKey = new File(WORKER_DIR + File.separator + tableName + File.separator + encode(rowName));
                if (!rowKey.exists()) {
                    res.status(404, "Not Found");
                    return "Not Found";
                }

                ByteArrayInputStream inputStream = new ByteArrayInputStream(Files.readAllBytes(rowKey.toPath()));
                row = Row.readFrom(inputStream);
                if (row == null) {
                    res.status(500, "Internal Server Error");
                    return "Bad reading from file";
                }

            } else {
                if (tables.get(tableName) == null) {
                    res.status(404, "Not Found");
                    return "Not Found";
                }

                row = tables.get(tableName).getRow(rowName);
                if (row == null) {
                    res.status(404, "Not Found");
                    return "Not Found";
                }
            }

            StringBuilder content = new StringBuilder();
            String[] columns = row.columns().toArray(new String[0]);
            Arrays.sort(columns);

            content.append(rowName).append(" ");
            for (String column : columns) {
                content.append(column).append(" ");
                content.append(row.get(column).length()).append(" ");
                content.append(row.get(column)).append(" ");
            }

            res.type("text/plain");
            res.status(200, "OK");
            res.body(content.toString());
            return null;
        });

        server.get("/data/:table", (req, res) -> {
            String tableName = req.params("table");
            String startRow = req.queryParams("startRow") == null ? null : req.queryParams("startRow");
            String endRow = req.queryParams("endRowExclusive") == null ? null : req.queryParams("endRowExclusive");

            if (tableName.startsWith("pt-")) {
                File permTable = new File(WORKER_DIR + File.separator + tableName);
                if (!permTable.exists()) {
                    res.status(404, "Not Found");
                    return "Not Found";
                }

                for (String rowKey : Objects.requireNonNull(permTable.list())) {
                    if ((startRow == null || rowKey.compareTo(startRow) >= 0) && (endRow == null || rowKey.compareTo(endRow) < 0)) {
                        File rowFile = new File(WORKER_DIR + File.separator + tableName + File.separator + rowKey);

                        ByteArrayInputStream inputStream = new ByteArrayInputStream(Files.readAllBytes(rowFile.toPath()));
                        Row row = Row.readFrom(inputStream);
                        if (row == null) {
                            res.status(500, "Internal Server Error");
                            return "Bad reading from file";
                        }
                        try { res.write(row.toByteArray()); }
                        catch (Exception e) {
                            res.status(500, "Internal Server Error");
                            return "Internal Server Error";
                        }
                    }
                }
                res.write("\n".getBytes());
                res.type("text/plain");
                res.status(200, "OK");
                return null;
            }
            Table table = tables.get(tableName);
            if (table == null) {
                res.status(404, "Not Found");
                return "Not Found";
            }

            String[] keys = table.sortedRows();
            for (String key : keys) {
                if ((startRow == null || key.compareTo(startRow) >= 0) && (endRow == null || key.compareTo(endRow) < 0)) {
                    try { res.write(table.getRow(key).toByteArray()); }
                    catch (Exception e) {
                        res.status(500, "Internal Server Error");
                        return "Internal Server Error";
                    }
                }
            }
            res.write("\n".getBytes());
            res.type("text/plain");
            res.status(200, "OK");
            return null;
        });

        server.get("/count/:table", (req, res) -> {
            String tableName = req.params("table");

            if (tableName.startsWith("pt-")) {
                File permTable = new File(WORKER_DIR + File.separator + tableName);
                if (!permTable.exists()) {
                    res.status(404, "Not Found");
                    return "Not Found";
                }
                res.status(200, "OK");
                return Integer.toString(Objects.requireNonNull(permTable.list()).length);
            }

            Table table = tables.get(tableName);
            if (table == null) {
                res.status(404, "Not Found");
                return "Not Found";
            }
            res.status(200, "OK");
            return Integer.toString(table.rows.size());
        });

        server.put("/rename/:table", (req, res) -> {
            String tableName = req.params("table");
            String newName = req.body();

            if (tableName.startsWith("pt-") && newName.startsWith("pt-")) {
                File permTable = new File(WORKER_DIR + File.separator + tableName);
                if (!permTable.exists()) {
                    res.status(404, "Not Found");
                    return "Not Found";
                }

                Files.move(permTable.toPath(), new File(WORKER_DIR + File.separator + newName).toPath());
                res.status(200, "OK");
                return "OK";
            }

            if (tableName.startsWith("pt-")) {
                File permTable = new File(WORKER_DIR + File.separator + tableName);
                if (!permTable.exists()) {
                    res.status(404, "Not Found");
                    return "Not Found";
                }

                Table table = new Table(newName);
                for (File rowFile: Objects.requireNonNull(permTable.listFiles())) {
                    Row row = Row.readFrom(new FileInputStream(rowFile));
                    if (row == null) {
                        res.status(500, "Internal Server Error");
                        return "cannot read from row";
                    }

                    table.insertRow(row);
                }
                tables.put(newName, table);
                if (!(permTable.delete())) {
                    res.status(500, "Internal Server Error");
                    return "Cannot delete permTable";
                }
                res.status(200, "OK");
                return "OK";
            }

            if (newName.startsWith("pt-")) {
                File newPermTable = new File(WORKER_DIR + File.separator + newName);
                if (newPermTable.exists()) {
                    res.status(409, "Conflict");
                    return "File name existed";
                }

                if (!(newPermTable.mkdir())) {
                    res.status(500, "Internal Server Error");
                    return "Cannot create permTable";
                }

                Table table = tables.get(tableName);
                if (table == null) {
                    res.status(404, "Not Found");
                    return "Not Found";
                }

                try {
                    table.rows.values().forEach(row -> {
                        File rowFile = new File(WORKER_DIR + File.separator + newName + File.separator + encode(row.key()));
                        FileOutputStream outputStream;
                        try {
                            outputStream = new FileOutputStream(rowFile);
                            outputStream.write(row.toByteArray());
                            outputStream.close();
                        } catch (IOException e) {
                            throw new RuntimeException("Cannot write to file");
                        }
                    });
                } catch (Exception e) {
                    res.status(500, "Internal Server Error");
                    return e.getMessage();
                }

                tables.remove(tableName);
                res.status(200, "OK");
                return "OK";
            }

            Table table = tables.get(tableName);
            if (tables.get(newName) != null) {
                res.status(409, "Conflict");
                return "Table name existed";
            }

            if (table == null) {
                res.status(404, "Not Found");
                return "Not Found";
            }
            tables.put(newName, table);
            tables.remove(tableName);

            res.status(200, "OK");
            return "OK";
        });

        server.put("/delete/:table", (req, res) -> {
            String table = req.params("table");

            if (table.startsWith("pt-")) {
                File permTable = new File(WORKER_DIR + File.separator + table);
                if (!permTable.exists()) {
                    res.status(404, "Not Found");
                    return "Not Found";
                }

                for (String row : Objects.requireNonNull(permTable.list())) {
                    if (!new File(WORKER_DIR + File.separator + table + File.separator + row).delete()) {
                        res.status(500, "Internal Server Error");
                        return "Internal Server Error";
                    }
                }

                if (!permTable.delete()) {
                    res.status(500, "Internal Server Error");
                    return "Internal Server Error";
                }
                res.status(200, "OK");
                return "OK";
            }

            if (tables.get(table) == null) {
                res.status(404, "Not Found");
                return "Not Found";
            }
            tables.remove(table);
            res.status(200, "OK");
            return "OK";
        });
    }
}

