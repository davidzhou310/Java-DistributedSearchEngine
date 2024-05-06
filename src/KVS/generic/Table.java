package KVS.generic;

import KVS.kvs.Row;

import java.util.Arrays;
import java.util.HashMap;
import java.util.TreeSet;

public class Table {
    public String name;
    public HashMap<String, Row> rows;
    public TreeSet<String> columns;
    public Table(String nameArg) {
        name = nameArg;
        rows = new HashMap<>();
        columns = new TreeSet<>();
    }
    public synchronized void insertRow(Row row) {
        columns.addAll(row.columns());
        rows.put(row.key(), row);
    }
    public synchronized Row getRow(String key) {
        for (String rowKey : rows.keySet()) {
            if (rowKey.equals(key)) {
                return rows.get(rowKey);
            }
        }
        return null;
    }
    public synchronized String[] sortedRows() {
        String[] keyset = rows.keySet().toArray(new String[0]);
        Arrays.sort(keyset);
        return keyset;
    }
}
