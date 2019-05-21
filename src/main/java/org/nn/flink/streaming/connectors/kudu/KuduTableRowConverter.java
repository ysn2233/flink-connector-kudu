package org.nn.flink.streaming.connectors.kudu;

import java.io.Serializable;

public interface KuduTableRowConverter<IN> extends Serializable {

    TableRow convert(IN value);

}
