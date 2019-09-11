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
import java.util.concurrent.CountDownLatch;

class PostgresSagaLogPool extends AbstractSagaLogPool {

    final Random random = new Random();
    final HikariDataSource dataSource;
    final String schema;
    final String namespace;
    final String clusterInstanceId;
    final Map<SagaLogId, SagaLog> connectedLocalLogBySagaLogId = new ConcurrentHashMap<>();
    final CountDownLatch shutdownSignal = new CountDownLatch(1);

    PostgresSagaLogPool(HikariDataSource dataSource, String schema, String namespace, String clusterInstanceId) {
        super(clusterInstanceId);
        new PostgresSagaLogId(schema, namespace, clusterInstanceId, "configuration-validation"); // valid if no exception is thrown
        this.dataSource = dataSource;
        this.schema = schema;
        this.namespace = namespace;
        this.clusterInstanceId = clusterInstanceId;
        initSchemaAndLocksTable();
    }

    private void initSchemaAndLocksTable() {
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
        String sql = String.format("CREATE SCHEMA IF NOT EXISTS \"%s\" AUTHORIZATION \"%s\"", schema, username);
        st.executeUpdate(sql);
        st.close();
    }

    static void createLocksTableIfNotExists(Connection connection, String schema) throws SQLException {
        Statement st = connection.createStatement();
        String sql = String.format("CREATE TABLE IF NOT EXISTS \"%s\".\"Locks\" (\n" +
                "    namespace       varchar NOT NULL,\n" +
                "    instance_id     varchar NOT NULL,\n" +
                "    log_id          varchar NOT NULL,\n" +
                "    lock_key        bigint  NOT NULL,\n" +
                "    PRIMARY KEY (namespace, instance_id, log_id)\n" +
                ")", schema);
        st.executeUpdate(sql);
        st.close();
    }

    @Override
    public PostgresSagaLogId idFor(String clusterInstanceId, String logName) {
        return new PostgresSagaLogId(schema, namespace, clusterInstanceId, logName);
    }

    @Override
    public Set<SagaLogId> clusterWideLogIds() {
        try (Connection connection = dataSource.getConnection()) {
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
            return logIds;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected SagaLog connectExternal(SagaLogId logId) throws SagaLogBusyException {
        return connectedLocalLogBySagaLogId.computeIfAbsent(logId, id -> {
            long key = getOrAssignLockKey(id);
            boolean success = tryLock(key);
            if (!success) {
                throw new SagaLogBusyException(String.format("Unable to acquire exclusive lock on %s with lock-key %d.", id, key));
            }
            return new PostgresSagaLog(dataSource, id, key);
        });
    }

    @Override
    protected boolean deleteExternal(SagaLogId logId) {
        SagaLog sagaLog = connectedLocalLogBySagaLogId.remove(logId);
        if (sagaLog != null) {
            try {
                sagaLog.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                unlock(((PostgresSagaLog) sagaLog).lockKey);
            }
        }
        return sagaLog != null;
    }

    private boolean tryLock(long key) {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(true);
            String sql = String.format("SELECT pg_try_advisory_lock(?)");
            PreparedStatement ps = connection.prepareStatement(sql);
            ps.setLong(1, key);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                boolean obtained = rs.getBoolean(1);
                return obtained;
            }
            return false;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean unlock(long key) {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(true);
            String sql = String.format("SELECT pg_advisory_unlock(?)");
            PreparedStatement ps = connection.prepareStatement(sql);
            ps.setLong(1, key);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                boolean released = rs.getBoolean(1);
                return released;
            }
            return false;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private long getOrAssignLockKey(SagaLogId logId) {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            connection.beginRequest();
            boolean success = false;
            try {
                long lockKey = getOrAssignLockKey(connection, logId);
                success = true;
                return lockKey;
            } finally {
                if (success) {
                    connection.commit();
                }
                connection.endRequest();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private long getOrAssignLockKey(Connection connection, SagaLogId logId) throws SQLException {
        Long existingLockKey = findExistingLock(connection, logId);
        if (existingLockKey != null) {
            return existingLockKey;
        }

        // assigned new lock
        long lockKey = findAvailableLockKey(connection);
        assignLockKeyToLog(connection, logId, lockKey);
        return lockKey;
    }

    private Long findExistingLock(Connection connection, SagaLogId logId) throws SQLException {
        String sql = String.format("SELECT lock_key FROM \"%s\".\"Locks\" WHERE namespace = ? AND instance_id = ? AND log_id = ?", ((PostgresSagaLogId) logId).getSchema());
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.setString(1, namespace);
        ps.setString(2, clusterInstanceId);
        ps.setString(3, logId.getLogName());
        ResultSet rs = ps.executeQuery();
        if (rs.next()) {
            Long lockKey = (Long) rs.getObject(1);
            ps.close();
            return lockKey;
        }
        ps.close();
        return null;
    }

    private long findAvailableLockKey(Connection connection) throws SQLException {
        String sql = String.format("SELECT 1 FROM \"%s\".\"Locks\" WHERE lock_key = ?", schema);
        PreparedStatement ps = connection.prepareStatement(sql);
        for (int i = 0; i < 100; i++) {
            long attemptedLockKey = 1 + random.nextInt(1000000000);
            ps.setLong(1, attemptedLockKey);
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) {
                rs.close();
                return attemptedLockKey;
            }
            rs.close();
        }
        ps.close();
        throw new IllegalStateException("Unable to generate random lockKey that is available");
    }

    private void assignLockKeyToLog(Connection connection, SagaLogId logId, long lockKey) throws SQLException {
        String sql = String.format("UPDATE \"%s\".\"Locks\" SET lock_key = ? WHERE namespace = ? AND instance_id = ? AND log_id = ?", ((PostgresSagaLogId) logId).getSchema());
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.setLong(1, lockKey);
        ps.setString(2, namespace);
        ps.setString(3, clusterInstanceId);
        ps.setString(4, logId.getLogName());
        ps.executeUpdate();
        ps.close();
    }

    @Override
    public void shutdown() {
        shutdownSignal.countDown();
    }
}
