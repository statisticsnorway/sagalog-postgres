import no.ssb.sagalog.SagaLogInitializer;
import no.ssb.sagalog.postgres.PostgresSagaLogInitializer;

module no.ssb.rawdata.postgres {
    requires no.ssb.sagalog;
    requires java.logging;
    requires org.slf4j;
    requires java.sql;
    requires de.huxhorn.sulky.ulid;
    requires com.zaxxer.hikari;
    requires postgresql;

    opens postgres;

    provides SagaLogInitializer with PostgresSagaLogInitializer;
}
