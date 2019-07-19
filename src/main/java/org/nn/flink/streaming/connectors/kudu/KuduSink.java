package org.nn.flink.streaming.connectors.kudu;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

/**
 *
 * @param <IN> Event type
 */
public class KuduSink<IN> extends RichSinkFunction<IN> {

    private static Logger logger = LoggerFactory.getLogger(KuduSink.class);

    private Configuration kuduConfig = new Configuration();
    private String masterAddress;
    private String tableName;
    private KuduTable table;
    private KuduTableRowConverter<IN> kuduTableRowConverter;
    private transient KuduSession session;
    private transient KuduClient client;
    private TableSerializationSchema<IN> tableNameSerial;

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
     * @param tableNameSerial Custom serialization tableNameSerial to sink to multiple tables
     * @param converter Custom converter to convert <IN> record to {@link TableRow}
     * @param properties Kudu configuration
     */
    public KuduSink(String masterAddress, TableSerializationSchema<IN> tableNameSerial, KuduTableRowConverter<IN> converter, Properties properties) {
        this(properties);
        this.masterAddress = masterAddress;
        this.tableNameSerial = tableNameSerial;
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
    public void open(Configuration parameters) throws KuduException {
        this.client = new KuduClient.KuduClientBuilder(masterAddress).build();
        this.session = client.newSession();
        session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
        session.setTimeoutMillis(kuduConfig.getLong("timeoutMillis", AsyncKuduClient.DEFAULT_OPERATION_TIMEOUT_MS));
        session.setIgnoreAllDuplicateRows(kuduConfig.getBoolean("ignoreDuplicateRows", true));
        session.setMutationBufferSpace(kuduConfig.getInteger("batchSize", 1000));
        if (tableName != null) {
            this.table = client.openTable(tableName);
        }
    }

    @Override
    public void invoke(IN row, Context context){
        System.out.println(row);
        Insert insert;
        try {
            if (tableNameSerial != null)
                table = client.openTable(tableNameSerial.serializeTableName(row));
            insert = table.newInsert();
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Error open kudu table for insertion", e);
            return;
        }
        try {
            List<ColumnSchema> columns = table.getSchema().getColumns();
            TableRow tableRow = kuduTableRowConverter.convert(row);
            for (ColumnSchema columnSchema : columns) {
                PartialRow partialRow = insert.getRow();
                String columnName = columnSchema.getName().toLowerCase();
                Type type = columnSchema.getType();
                if (tableRow.getPairs().containsKey(columnName)) {
                    Object value = tableRow.getElement(columnName);
                    KuduMapper.rowAdd(partialRow, columnName, value, type);
                }
            }
            session.apply(insert);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Error inserting table", e);
        }

    }

    @Override
    public void close() throws KuduException {
        this.session.close();
        this.client.close();
    }

}
