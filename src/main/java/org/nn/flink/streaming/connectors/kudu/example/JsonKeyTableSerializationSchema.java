package org.nn.flink.streaming.connectors.kudu.example;

import com.alibaba.fastjson.JSONObject;
import org.nn.flink.streaming.connectors.kudu.TableSerializationSchema;

public class JsonKeyTableSerializationSchema implements TableSerializationSchema<JSONObject> {

    private static final long serialVersionUID = 1L;

    private String prefix;

    private String suffix;

    private String key;

    public JsonKeyTableSerializationSchema(String key, String prefix, String suffix) {
        this.key = key;
        this.prefix = prefix;
        this.suffix = suffix;
    }

    @Override
    public String serializeTable(JSONObject value) {
        return prefix + value.getString(key).toLowerCase() + suffix;
    }
}
