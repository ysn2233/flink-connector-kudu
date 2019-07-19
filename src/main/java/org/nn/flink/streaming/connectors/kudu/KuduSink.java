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
    private transient AsyncKuduSession session;
    private transient AsyncKuduClient client;
    private KuduMapper.Mode mode = KuduMapper.Mode.UPSERT;
    private TableSerializationSchema<IN> tableNameSerial;

    /**
     *
     * @param masterAddress Master address of Kudu
     * @param tableName Table name if sink to single table
     * @param mode Kudu operation mode {INSERT, UPDATE, UPSERT}
     * @param converter Custom converter to convert <IN> record to {@link TableRow}
     * @param properties Kudu configuration
     */
    public KuduSink(String masterAddress, String tableName, KuduMapper.Mode mode, KuduTableRowConverter<IN> converter, Properties properties) {
        this(masterAddress, tableName, converter, properties);
        this.mode = mode;
        setConfig(properties);
    }

    /**
     *
     * @param masterAddress Master address of Kudu
     * @param tableName Table name if sink to single table
     * @param converter Custom converter to convert <IN> record to {@link TableRow}
     * @param properties Kudu configuration
     */
    public KuduSink(String masterAddress, String tableName, KuduTableRowConverter<IN> converter, Properties properties) {
        this.masterAddress = masterAddress;
        this.tableName = tableName;
        this.kuduTableRowConverter = converter;
        setConfig(properties);
    }

    /**
     *
     * @param masterAddress Master address of Kudu
     * @param tableNameSerial Custom serialization tableNameSerial to sink to multiple tables
     * @param mode Kudu operation mode {INSERT, UPDATE, UPSERT}
     * @param converter Custom converter to convert <IN> record to {@link TableRow}
     * @param properties Kudu configuration
     */
    public KuduSink(String masterAddress, TableSerializationSchema<IN> tableNameSerial,
                    KuduTableRowConverter<IN> converter, Properties properties) {
        this.masterAddress = masterAddress;
        this.tableNameSerial = tableNameSerial;
        this.kuduTableRowConverter = converter;
        setConfig(properties);
    }

    /**
     *
     * @param masterAddress Master address of Kudu
     * @param tableNameSerial Custom serialization tableNameSerial to sink to multiple tables
     * @param mode Kudu operation mode {INSERT, UPDATE, UPSERT}
     * @param converter Custom converter to convert <IN> record to {@link TableRow}
     * @param properties Kudu configuration
     */
    public KuduSink(String masterAddress, TableSerializationSchema<IN> tableNameSerial,
                    KuduMapper.Mode mode, KuduTableRowConverter<IN> converter, Properties properties) {

        this(masterAddress, tableNameSerial, converter, properties);
        this.mode = mode;
        setConfig(properties);
    }

    public void setConfig(Properties properties) {
        kuduConfig.setLong("timeoutMillis", Long.valueOf(properties.getProperty("timeoutMillis", "30000")));
        kuduConfig.setBoolean("ignoreDuplicateRows", Boolean.valueOf(properties.getProperty("ignoreDuplicateRows", "true")));
        kuduConfig.setInteger("batchSize", Integer.valueOf(properties.getProperty("batchSize", "1000")));
    }

    @Override
    public void open(Configuration parameters) throws KuduException {
        this.client = new AsyncKuduClient.AsyncKuduClientBuilder(masterAddress).build();
        this.session = client.newSession();
        session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
        session.setTimeoutMillis(kuduConfig.getLong("timeoutMillis", AsyncKuduClient.DEFAULT_OPERATION_TIMEOUT_MS));
        session.setIgnoreAllDuplicateRows(kuduConfig.getBoolean("ignoreDuplicateRows", true));
        session.setMutationBufferSpace(kuduConfig.getInteger("batchSize", 1000));
        if (tableName != null) {
            this.table = client.syncClient().openTable(tableName);
        }
    }

    @Override
    public void invoke(IN row, Context context){
        Insert insert;
        try {
            if (tableNameSerial != null)
                table = client.syncClient().openTable(tableNameSerial.serializeTableName(row));
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Error open kudu table for insertion", e);
            return;
        }
        try {
            TableRow tableRow = kuduTableRowConverter.convert(row);
            Operation op = KuduMapper.rowOperation(tableRow, table, mode);
            session.apply(op);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Error inserting table", e);
        }

    }

    @Override
    public void close() throws Exception {
        this.session.close();
        this.client.close();
    }

}
