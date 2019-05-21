# Flink-connector-kudu

Apache Flink Kudu connector which provides sink recrods of Dataset and DataStream to kudu tables.

## Usage
This is an straming example which sinks JSONObject records to kudu table. The example class is org.nn.flink.streaming.connectors.kudu.example.KuduSinkExample. Batch example can be found as org.nn.flink.streaming.connectors.kudu.example.KuduBatchOutputExample.

``` java

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStream<String> stream = env.socketTextStream("localhost", 44444);

    Properties properties = new Properties();
    properties.setProperty("timeoutMillis", "50000");
    properties.setProperty("batchSize", "1000");
    System.out.println(pt.get("masterAddress"));
    KuduSink<JSONObject> kudu = new KuduSink<JSONObject>(
        MASTER_ADDRESS,
        TABLE_NAME,
        new JsonKuduTableRowConverter(),
        properties
    )

    stream
        .map(s -> (JSONObject)JSONObject.parse(s))
        .addSink(kudu);
    env.execute("kudu");
```
`JsonKuduTableRowConverter` is a class implements `KuduTableRowConverter` interface and provide the conversion between record type `<IN>` and `TableRow`

## Multiple-tables
This library supports sink to multiple tables based on custom stretegy. Here a example to achieve sinking JSONObject record to multiple tables based on the value of a certain key.
``` java
    KuduSink<JSONObject> kudu = new KuduSink<JSONObject>(
        MASTER_ADDRESS,
        new JsonKeyTableSerializationSchema(),
        new JsonKuduTableRowConverter(),
        properties
    )
```
`JsonKeyTableSerializationSchema` implements `TableSerializationSchema` interface. `serializeTable()` method should include details that how tableName is determined.