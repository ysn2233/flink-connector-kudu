package org.nn.flink.streaming.connectors.kudu;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class KuduOutputFormat<IN> extends RichOutputFormat<IN> {


    private static Logger logger = LoggerFactory.getLogger(KuduOutputFormat.class);

    private Configuration kuduConfig = new Configuration();
    private String masterAddress;
    private String tableName;
    private KuduTableRowConverter<IN> kuduTableRowConverter;
    private transient KuduSession session;
    private transient KuduClient client;
    private TableSerializationSchema<IN> schema;

    /**
     *
     * @param masterAddress Master address of Kudu
     * @param tableName Table name if sink to single table
     * @param converter Custom converter to convert <IN> record to {@link TableRow}
     * @param properties Kudu configuration
     */
    public KuduOutputFormat(String masterAddress, String tableName, KuduTableRowConverter<IN> converter, Properties properties) {
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
    public KuduOutputFormat(String masterAddress, TableSerializationSchema schema , KuduTableRowConverter<IN> converter, Properties properties) {
        this(properties);
        this.masterAddress = masterAddress;
        this.schema = schema;
        this.kuduTableRowConverter = converter;
    }

    public KuduOutputFormat(Properties properties) {
        this.masterAddress =  properties.getProperty("masterAddress");
        this.tableName = properties.getProperty("tableName");
        kuduConfig.setLong("timeoutMillis", Long.valueOf(properties.getProperty("timeoutMillis", "30000")));
        kuduConfig.setBoolean("ignoreDuplicateRows", Boolean.valueOf(properties.getProperty("ignoreDuplicateRows", "true")));
        kuduConfig.setInteger("batchSize", Integer.valueOf(properties.getProperty("batchSize", "1000")));
    }

    @Override
    public void configure(Configuration parameters) {

    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        this.client = new KuduClient.KuduClientBuilder(masterAddress).build();
        this.session = client.newSession();
        session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
        session.setTimeoutMillis(kuduConfig.getLong("timeoutMillis", AsyncKuduClient.DEFAULT_OPERATION_TIMEOUT_MS));
        session.setIgnoreAllDuplicateRows(kuduConfig.getBoolean("ignoreDuplicateRows", true));
        session.setMutationBufferSpace(kuduConfig.getInteger("batchSize", 1000));
    }

    @Override
    public void writeRecord(IN row) throws IOException {
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

    /**
     * Method that marks the end of the life-cycle of parallel output instance. Should be used to close
     * channels and streams and release resources.
     * After this method returns without an error, the output is assumed to be correct.
     * <p>
     * When this method is called, the output format it guaranteed to be opened.
     *
     * @throws IOException Thrown, if the input could not be closed properly.
     */
    @Override
    public void close() throws IOException {
        this.session.close();
        this.client.close();
    }
}
