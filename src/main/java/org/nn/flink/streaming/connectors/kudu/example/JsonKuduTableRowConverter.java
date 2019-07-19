package org.nn.flink.streaming.connectors.kudu.example;

import com.alibaba.fastjson.JSONObject;
import org.nn.flink.streaming.connectors.kudu.KuduTableRowConverter;
import org.nn.flink.streaming.connectors.kudu.TableRow;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

public class JsonKuduTableRowConverter implements KuduTableRowConverter<JSONObject> {

    private static final long serialVersionUID = 1L;

    @Override
    public TableRow convert(JSONObject value) {
        TableRow tableRow = new TableRow();
        for (Map.Entry<String, Object> entry : value.entrySet()) {
            tableRow.putElement(entry.getKey(), entry.getValue());
        }
        try {
            byte[] idb = MessageDigest.getInstance("md5").digest(value.toJSONString().getBytes(StandardCharsets.UTF_8));
            String id = new String(idb);
            tableRow.putElement("kudu_uuid", id);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return tableRow;
    }
}
