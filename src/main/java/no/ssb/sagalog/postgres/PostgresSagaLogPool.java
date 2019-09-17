package no.ssb.sagalog.postgres;

import com.zaxxer.hikari.HikariDataSource;
import no.ssb.sagalog.AbstractSagaLogPool;
import no.ssb.sagalog.SagaLog;
import no.ssb.sagalog.SagaLogBusyException;
import no.ssb.sagalog.SagaLogId;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

class PostgresSagaLogPool extends AbstractSagaLogPool {

    final Random random = new Random();
    final HikariDataSource dataSource;
    final String schema;
    final String namespace;
    final String clusterInstanceId;
    final Map<SagaLogId, SagaLog> connectedLocalLogBySagaLogId = new ConcurrentHashMap<>();

    PostgresSagaLogPool(HikariDataSource dataSource, String schema, String namespace, String clusterInstanceId) {
        super(clusterInstanceId);
        new PostgresSagaLogId(schema, namespace, clusterInstanceId, "configuration-validation"); // valid if no exception is thrown
        this.dataSource = dataSource;
        this.schema = schema;
        this.namespace = namespace;
        this.clusterInstanceId = clusterInstanceId;
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            connection.beginRequest();
            createSchemaIfNotExists(connection, schema, "test");
            createLocksTableIfNotExists(connection, schema);
            connection.commit();
            connection.endRequest();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    static void createSchemaIfNotExists(Connection connection, String schema, String username) throws SQLException {
        Statement st = connection.createStatement();
        try {
            String sql = String.format("CREATE SCHEMA IF NOT EXISTS \"%s\" AUTHORIZATION \"%s\"", schema, username);
            st.executeUpdate(sql);
        } finally {
            st.close();
        }
    }

    static void createLocksTableIfNotExists(Connection connection, String schema) throws SQLException {
        Statement st = connection.createStatement();
        try {
            String sql = String.format("CREATE TABLE IF NOT EXISTS \"%s\".\"Locks\" (\n" +
                    "    namespace       varchar NOT NULL,\n" +
                    "    instance_id     varchar NOT NULL,\n" +
                    "    log_id          varchar NOT NULL,\n" +
                    "    lock_key        bigint  NOT NULL,\n" +
                    "    PRIMARY KEY (namespace, instance_id, log_id)\n" +
                    ")", schema);
            st.executeUpdate(sql);
        } finally {
            st.close();
        }
    }

    @Override
    public PostgresSagaLogId idFor(String clusterInstanceId, String logName) {
        return new PostgresSagaLogId(schema, namespace, clusterInstanceId, logName);
    }

    @Override
    public Set<SagaLogId> clusterWideLogIds() {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(true);
            connection.beginRequest();
            PreparedStatement ps = connection.prepareStatement("SELECT tablename FROM pg_tables WHERE schemaname = ? AND starts_with(tablename, ?)");
            ps.setString(1, schema);
            String tableNamePrefix = "SAGALOG_" + namespace + "_";
            ps.setString(2, tableNamePrefix);
            ResultSet rs = ps.executeQuery();
            Set<SagaLogId> logIds = new LinkedHashSet<>();
            while (rs.next()) {
                String tableName = rs.getString(1);
                PostgresSagaLogId sagaLogId = new PostgresSagaLogId(schema, tableName);
                logIds.add(sagaLogId);
            }
            connection.endRequest();
            return logIds;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected SagaLog connectExternal(SagaLogId logId) throws SagaLogBusyException {
        return connectedLocalLogBySagaLogId.computeIfAbsent(logId, id -> new PostgresSagaLog(dataSource, id, random, 0));
    }

    @Override
    protected boolean deleteExternal(SagaLogId logId) {
        SagaLog sagaLog = connectedLocalLogBySagaLogId.remove(logId);
        if (sagaLog != null) {
            try {
                sagaLog.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return sagaLog != null;
    }

    @Override
    public void shutdown() {
        for (Map.Entry<SagaLogId, SagaLog> entry : connectedLocalLogBySagaLogId.entrySet()) {
            try {
                entry.getValue().close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
