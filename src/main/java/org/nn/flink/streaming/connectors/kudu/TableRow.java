package org.nn.flink.streaming.connectors.kudu;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class TableRow implements Serializable {

    private static final long serialVersionUID = 1L;

    private Map<String, Object> pairs = new HashMap<>();

    public int size() {
        return pairs.size();
    }

    public Map<String, Object> getPairs() {
        return pairs;
    }

    public Object getElement(String key) {
        return pairs.get(key);
    }

    public void putElement(String key, Object value) {
        pairs.put(key, value);
    }

}
