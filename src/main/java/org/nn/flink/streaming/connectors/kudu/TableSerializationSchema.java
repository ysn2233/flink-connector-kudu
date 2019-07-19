package org.nn.flink.streaming.connectors.kudu;

import java.io.Serializable;

public interface TableSerializationSchema<IN> extends Serializable {

    String serializeTableName(IN value);

}
