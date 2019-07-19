package org.nn.flink.streaming.connectors.kudu.example;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.nn.flink.streaming.connectors.kudu.KuduSink;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * A sink example of kudu
 */
public class KuduSinkExample {

    public static void main(String [] args) throws Exception {

        ParameterTool pt = ParameterTool.fromArgs(args);
        Configuration conf = new Configuration();
        conf.setString("security.kerberos.login.keytab", pt.get("kerberos-keytab"));
        conf.setString("security.kerberos.login.principal", pt.get("kerberos-principal"));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1, conf);

        DataStream<String> stream = env.socketTextStream("localhost", 44444);

        Properties properties = new Properties();
        properties.setProperty("timeoutMillis", "50000");
        properties.setProperty("batchSize", "1000");
        System.out.println(pt.get("masterAddress"));
        KuduSink<JSONObject> kudu = new KuduSink<JSONObject>(
                pt.get("masterAddress"),
                new JsonKeyTableSerializationSchema("table_name", "impala::default.", ""),
                new JsonKuduTableRowConverter(), properties);

        stream
                .map(s -> (JSONObject)JSONObject.parse(s))
                .addSink(kudu);
        env.execute("kudu");

    }
}
