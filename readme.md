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

# 中文文档

Flink-kudu-connector提供DataStream和DataSet sink到Kudu table中的操作。

# 使用
下面的例子将包含JSONObject记录的DataStream sink到Kudu table中去。Example代码在org.nn.flink.streaming.connectors.kudu.example.KuduSinkExample类中. Dataset的example 在org.nn.flink.streaming.connectors.kudu.example.KuduBatchOutputExample类中.

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
`JsonKuduTableRowConverter`实现了`KuduTableRowConverter`接口，用于将某种类型的记录（这里是JSONObject）转化为TableRow来让代码处理。

## 多表支持
该库支持了自定义分配策略来将数据插入到不同的Kudu表中。下面是一个example实现了将JSON记录按某个key的值来插入到不同的表中去。
``` java
    KuduSink<JSONObject> kudu = new KuduSink<JSONObject>(
        MASTER_ADDRESS,
        new JsonKeyTableSerializationSchema("table_name", TABLE_PREFIX, TABLE_SUFFIX),
        new JsonKuduTableRowConverter(),
        properties
    )
```
`JsonKeyTableSerializationSchema`实现了`TableSerializationSchema`接口. 需要重写的`serializeTable()`用于实现插入表明的判断逻辑。