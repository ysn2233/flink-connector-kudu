package org.nn.flink.streaming.connectors.kudu;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 *
 * @param <IN> Event type
 */
public class KuduSink<IN> extends RichSinkFunction<IN> {

    private static Logger logger = LoggerFactory.getLogger(KuduSink.class);

    private Configuration kuduConfig = new Configuration();
    private String masterAddress;
    private String tableName;
    private KuduTableRowConverter<IN> kuduTableRowConverter;
    private KuduSession session;
    private KuduClient client;
    private TableSerializationSchema<IN> schema;

    /**
     *
     * @param masterAddress Master address of Kudu
     * @param tableName Table name if sink to single table
     * @param converter Custom converter to convert <IN> record to {@link TableRow}
     * @param properties Kudu configuration
     */
    public KuduSink(String masterAddress, String tableName, KuduTableRowConverter<IN> converter, Properties properties) {
        this(properties);
        this.masterAddress = masterAddress;
        this.tableName = tableName;
        this.kuduTableRowConverter = converter;
    }

    /**
     *
     * @param masterAddress Master address of Kudu
     * @param schema Custom serialization schema to sink to multiple tables
     * @param converter Custom converter to convert <IN> record to {@link TableRow}
     * @param properties Kudu configuration
     */
    public KuduSink(String masterAddress, TableSerializationSchema schema , KuduTableRowConverter<IN> converter, Properties properties) {
        this(properties);
        this.masterAddress = masterAddress;
        this.schema = schema;
        this.kuduTableRowConverter = converter;
    }

    public KuduSink(Properties properties) {
        this.masterAddress =  properties.getProperty("masterAddress");
        this.tableName = properties.getProperty("tableName");
        kuduConfig.setLong("timeoutMillis", Long.valueOf(properties.getProperty("timeoutMillis", "30000")));
        kuduConfig.setBoolean("ignoreDuplicateRows", Boolean.valueOf(properties.getProperty("ignoreDuplicateRows", "true")));
        kuduConfig.setInteger("batchSize", Integer.valueOf(properties.getProperty("batchSize", "1000")));
    }

    @Override
    public void open(Configuration parameters) {
        this.client = new KuduClient.KuduClientBuilder(masterAddress).build();
        this.session = client.newSession();
        session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
        session.setTimeoutMillis(kuduConfig.getLong("timeoutMillis", AsyncKuduClient.DEFAULT_OPERATION_TIMEOUT_MS));
        session.setIgnoreAllDuplicateRows(kuduConfig.getBoolean("ignoreDuplicateRows", true));
        session.setMutationBufferSpace(kuduConfig.getInteger("batchSize", 1000));
    }

    @Override
    public void invoke(IN row, Context context){
        Insert insert;
        KuduTable table;
        try {
            if (schema != null)
                table = client.openTable(schema.serializeTable(row));
            else
                table = client.openTable(tableName);
            insert = table.newInsert();
        } catch (Exception e) {
            logger.error("Error open kudu table for insertion", e);
            return;
        }
        try {
            List<ColumnSchema> columns = table.getSchema().getColumns();
            List<String> columnNames = columns.stream().map(c -> c.getName().toLowerCase()).collect(Collectors.toList());
            TableRow tableRow = kuduTableRowConverter.convert(row);
            for (String column: columnNames) {
                PartialRow partialRow = insert.getRow();
                if (tableRow.getPairs().containsKey(column)) {
                    Object value = tableRow.getElement(column);
                    KuduInsertUtils.rowAdd(partialRow, column, value);
                }
            }
            session.apply(insert);
        } catch (Exception e) {
            logger.error("Error inserting table", e);
        }

    }

    @Override
    public void close() throws KuduException {
        this.session.close();
        this.client.close();
    }

}
