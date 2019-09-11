package no.ssb.sagalog.postgres;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import no.ssb.sagalog.SagaLogInitializer;

import java.io.PrintWriter;
import java.util.Map;
import java.util.Properties;

public class PostgresSagaLogInitializer implements SagaLogInitializer {

    public PostgresSagaLogInitializer() {
    }

    @Override
    public PostgresSagaLogPool initialize(Map<String, String> configuration) {
        String clusterOwner = configuration.get("cluster.owner");
        String namespace = configuration.get("cluster.name");
        String instanceId = configuration.get("cluster.instance-id");

        HikariDataSource dataSource = createHikariDataSource(configuration);

        return new PostgresSagaLogPool(dataSource, clusterOwner, namespace, instanceId);

    }

    @Override
    public Map<String, String> configurationOptionsAndDefaults() {
        return Map.of(
                "cluster.owner", "mycompany",
                "cluster.name", "internal-sagalog-integration-testing",
                "cluster.instance-id", "01",
                "postgres.driver.host", "localhost",
                "postgres.driver.port", "5432",
                "postgres.driver.user", "test",
                "postgres.driver.password", "test",
                "postgres.driver.database", "sagalog"
        );
    }

    static HikariDataSource createHikariDataSource(Map<String, String> configuration) {
        boolean enableH2DatabaseDriver = Boolean.parseBoolean(configuration.get("h2.enabled"));

        HikariDataSource dataSource;

        if (enableH2DatabaseDriver) {
            dataSource = openH2DataSource(
                    configuration.get("h2.driver.url"),
                    "sa",
                    "sa"
            );
        } else {
            dataSource = openPostgresDataSource(
                    configuration.get("postgres.driver.host"),
                    configuration.get("postgres.driver.port"),
                    configuration.get("postgres.driver.user"),
                    configuration.get("postgres.driver.password"),
                    configuration.get("postgres.driver.database")
            );
        }
        return dataSource;
    }

    static HikariDataSource openPostgresDataSource(String postgresDbDriverHost, String postgresDbDriverPort, String postgresDbDriverUser, String postgresDbDriverPassword, String postgresDbDriverDatabase) {
        Properties props = new Properties();
        props.setProperty("dataSourceClassName", "org.postgresql.ds.PGSimpleDataSource");
        props.setProperty("dataSource.serverName", postgresDbDriverHost);
        props.setProperty("dataSource.portNumber", postgresDbDriverPort);
        props.setProperty("dataSource.user", postgresDbDriverUser);
        props.setProperty("dataSource.password", postgresDbDriverPassword);
        props.setProperty("dataSource.databaseName", postgresDbDriverDatabase);
        props.put("dataSource.logWriter", new PrintWriter(System.out));

        HikariConfig config = new HikariConfig(props);
        config.setAutoCommit(false);
        config.setMaximumPoolSize(10);
        HikariDataSource datasource = new HikariDataSource(config);

        return datasource;
    }

    static HikariDataSource openH2DataSource(String jdbcUrl, String username, String password) {
        Properties props = new Properties();
        props.setProperty("jdbcUrl", jdbcUrl);
        props.setProperty("username", username);
        props.setProperty("password", password);
        props.put("dataSource.logWriter", new PrintWriter(System.out));

        HikariConfig config = new HikariConfig(props);
        config.setAutoCommit(false);
        config.setMaximumPoolSize(10);
        HikariDataSource datasource = new HikariDataSource(config);

        return datasource;
    }
}
