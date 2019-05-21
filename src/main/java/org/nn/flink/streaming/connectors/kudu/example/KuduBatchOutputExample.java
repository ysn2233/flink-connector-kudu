package org.nn.flink.streaming.connectors.kudu.example;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.nn.flink.streaming.connectors.kudu.KuduOutputFormat;

import java.util.Properties;

/**
 * A sink example of kudu
 */
public class KuduBatchOutputExample {

    private static final String INPUT_FILE = "file:///path/to/your/file";

    public static void main(String [] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ParameterTool pt = ParameterTool.fromArgs(args);

        DataSet<String> data = env.readTextFile(INPUT_FILE);

        Properties properties = new Properties();
        properties.setProperty("timeoutMillis", "50000");
        properties.setProperty("batchSize", "1000");
        KuduOutputFormat<JSONObject> kudu = new KuduOutputFormat<>(
                pt.get("masterAddress"),
                new JsonKeyTableSerializationSchema("table_name", "impala::default.", ""),
                new JsonKuduTableRowConverter(), properties);

        data
                .map(s -> (JSONObject)JSONObject.parse(s))
                .output(kudu);
        env.execute("kudu");

    }
}
