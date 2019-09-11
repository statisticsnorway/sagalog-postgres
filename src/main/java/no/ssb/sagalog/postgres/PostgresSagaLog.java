package no.ssb.sagalog.postgres;

import de.huxhorn.sulky.ulid.ULID;
import no.ssb.sagalog.SagaLog;
import no.ssb.sagalog.SagaLogEntry;
import no.ssb.sagalog.SagaLogEntryBuilder;
import no.ssb.sagalog.SagaLogEntryId;
import no.ssb.sagalog.SagaLogEntryType;
import no.ssb.sagalog.SagaLogId;
import org.postgresql.util.PGobject;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

class PostgresSagaLog implements SagaLog, AutoCloseable {


    final ULID ulid = new ULID();
    final AtomicReference<ULID.Value> prevUlid = new AtomicReference<>(ulid.nextValue());

    final DataSource dataSource;
    final PostgresSagaLogId sagaLogId;
    final long lockKey;
    final AtomicBoolean closed = new AtomicBoolean(false);

    PostgresSagaLog(DataSource dataSource, SagaLogId _sagaLogId, long lockKey) {
        this.dataSource = dataSource;
        this.sagaLogId = (PostgresSagaLogId) _sagaLogId;
        this.lockKey = lockKey;
        initSagalogTable();
    }

    private void initSagalogTable() {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            connection.beginRequest();
            createSagalogTableIfNotExists(connection);
            connection.commit();
            connection.endRequest();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void createSagalogTableIfNotExists(Connection connection) throws SQLException {
        String sql = String.format("CREATE TABLE IF NOT EXISTS \"%s\".\"%s\" (\n" +
                "    entry_id        uuid      NOT NULL,\n" +
                "    txid            uuid      NOT NULL,\n" +
                "    entry_type      smallint  NOT NULL,\n" +
                "    node_id         varchar   NOT NULL,\n" +
                "    saga_name       varchar   NULL,\n" +
                "    data            json      NULL,\n" +
                "    PRIMARY KEY (entry_id, txid)\n" +
                ")", sagaLogId.getSchema(), sagaLogId.getTableName());
        connection.createStatement().executeUpdate(sql);
    }

    public ULID.Value generateId() {
        ULID.Value previousUlid = prevUlid.get();
        ULID.Value next = ulid.nextStrictlyMonotonicValue(previousUlid).orElse(null);
        while (next == null || !prevUlid.compareAndSet(previousUlid, next)) {
            Thread.yield();
            previousUlid = prevUlid.get();
            next = ulid.nextStrictlyMonotonicValue(previousUlid).orElse(null);
        }
        return next;
    }

    @Override
    public SagaLogId id() {
        return sagaLogId;
    }

    @Override
    public CompletableFuture<SagaLogEntry> write(SagaLogEntryBuilder builder) {
        checkNotClosed();
        return CompletableFuture.supplyAsync(() -> {
            ULID.Value entryId = generateId();
            SagaLogEntry entry = builder
                    .id(new PostgresSagaLogEntryId(entryId))
                    .build();
            try (Connection connection = dataSource.getConnection()) {
                connection.beginRequest();
                connection.setAutoCommit(true);
                String sql = String.format("INSERT INTO \"%s\".\"%s\" (txid, entry_id, entry_type, node_id, saga_name, data) VALUES(?, ?, ?, ?, ?, ?)", sagaLogId.getSchema(), sagaLogId.getTableName());
                PreparedStatement ps = connection.prepareStatement(sql);
                ps.setObject(1, UUID.fromString(entry.getExecutionId()));
                ps.setObject(2, new UUID(entryId.getMostSignificantBits(), entryId.getLeastSignificantBits()));
                ps.setShort(3, PostgresSagaTools.toShort(entry.getEntryType()));
                ps.setString(4, entry.getNodeId());
                ps.setString(5, entry.getSagaName());
                PGobject jsonData = new PGobject();
                jsonData.setType("json");
                jsonData.setValue(entry.getJsonData());
                ps.setObject(6, jsonData);
                ps.executeUpdate();
                connection.endRequest();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            return entry;
        });
    }

    @Override
    public CompletableFuture<Void> truncate(SagaLogEntryId _entryId) {
        checkNotClosed();
        return CompletableFuture.runAsync(() -> {
            try (Connection connection = dataSource.getConnection()) {
                connection.beginRequest();
                connection.setAutoCommit(true);
                String sql = String.format("DELETE FROM \"%s\".\"%s\" WHERE entry_id <= ?", sagaLogId.getSchema(), sagaLogId.getTableName());
                PreparedStatement ps = connection.prepareStatement(sql);
                PostgresSagaLogEntryId entryId = (PostgresSagaLogEntryId) _entryId;
                UUID uuid = new UUID(entryId.id.getMostSignificantBits(), entryId.id.getLeastSignificantBits());
                ps.setObject(1, uuid);
                ps.executeUpdate();
                connection.endRequest();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public CompletableFuture<Void> truncate() {
        checkNotClosed();
        return CompletableFuture.runAsync(() -> {
            try (Connection connection = dataSource.getConnection()) {
                connection.beginRequest();
                connection.setAutoCommit(true);
                String sql = String.format("TRUNCATE TABLE \"%s\".\"%s\"", sagaLogId.getSchema(), sagaLogId.getTableName());
                connection.createStatement().executeUpdate(sql);
                connection.endRequest();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public Stream<SagaLogEntry> readIncompleteSagas() {
        checkNotClosed();
        try (Connection connection = dataSource.getConnection()) {
            connection.beginRequest();
            connection.setAutoCommit(true);
            String sql = String.format("SELECT entry_id, txid, entry_type, node_id, saga_name, data FROM \"%s\".\"%s\"", sagaLogId.getSchema(), sagaLogId.getTableName());
            ResultSet rs = connection.createStatement().executeQuery(sql);
            List<SagaLogEntry> result = new ArrayList<>();
            while (rs.next()) {
                SagaLogEntry entry = sagaLogEntryRowMapper(rs);
                result.add(entry);
            }
            connection.endRequest();
            return result.stream();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Stream<SagaLogEntry> readEntries(String txId) {
        checkNotClosed();
        try (Connection connection = dataSource.getConnection()) {
            connection.beginRequest();
            connection.setAutoCommit(true);
            String sql = String.format("SELECT entry_id, txid, entry_type, node_id, saga_name, data FROM \"%s\".\"%s\" WHERE txid = ?", sagaLogId.getSchema(), sagaLogId.getTableName());
            PreparedStatement ps = connection.prepareStatement(sql);
            ps.setObject(1, UUID.fromString(txId));
            ResultSet rs = ps.executeQuery();
            List<SagaLogEntry> result = new ArrayList<>();
            while (rs.next()) {
                SagaLogEntry entry = sagaLogEntryRowMapper(rs);
                result.add(entry);
            }
            connection.endRequest();
            return result.stream();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private SagaLogEntry sagaLogEntryRowMapper(ResultSet rs) throws SQLException {
        UUID entry_id = (UUID) rs.getObject("entry_id");
        UUID txid = (UUID) rs.getObject("txid");
        short entry_type = rs.getShort("entry_type");
        String node_id = rs.getString("node_id");
        String saga_name = rs.getString("saga_name");
        String data = rs.getString("data");
        PostgresSagaLogEntryId entryId = new PostgresSagaLogEntryId(new ULID.Value(entry_id.getMostSignificantBits(), entry_id.getLeastSignificantBits()));
        String executionId = txid.toString();
        SagaLogEntryType entryType = PostgresSagaTools.fromShort(entry_type);
        return new SagaLogEntryBuilder()
                .executionId(executionId)
                .id(entryId)
                .entryType(entryType)
                .nodeId(node_id)
                .sagaName(saga_name)
                .jsonData(data)
                .build();
    }

    private void checkNotClosed() {
        if (closed.get()) {
            throw new RuntimeException(String.format("Saga-log is already closed, saga-log-id: %s", sagaLogId));
        }
    }

    @Override
    public String toString(SagaLogEntryId id) {
        return ((PostgresSagaLogEntryId) id).id.toString();
    }

    @Override
    public PostgresSagaLogEntryId fromString(String id) {
        return new PostgresSagaLogEntryId(ULID.parseULID(id));
    }

    @Override
    public byte[] toBytes(SagaLogEntryId id) {
        return ((PostgresSagaLogEntryId) id).id.toBytes();
    }

    @Override
    public SagaLogEntryId fromBytes(byte[] idBytes) {
        return new PostgresSagaLogEntryId(ULID.fromBytes(idBytes));
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
    }
}
