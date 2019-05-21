package org.nn.flink.streaming.connectors.kudu.example;

import com.alibaba.fastjson.JSONObject;
import org.nn.flink.streaming.connectors.kudu.KuduTableRowConverter;
import org.nn.flink.streaming.connectors.kudu.TableRow;

import java.util.Map;

public class JsonKuduTableRowConverter implements KuduTableRowConverter<JSONObject> {

    private static final long serialVersionUID = 1L;

    @Override
    public TableRow convert(JSONObject value) {
        TableRow tableRow = new TableRow();
        for (Map.Entry<String, Object> entry : value.entrySet()) {
            tableRow.putElement(entry.getKey(), entry.getValue());
        }
        return tableRow;
    }
}
