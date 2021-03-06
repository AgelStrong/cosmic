package com.cloud.flyway;

import com.cloud.legacymodel.exceptions.CloudRuntimeException;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.api.configuration.FluentConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlywayDB {

    private static final Logger logger = LoggerFactory.getLogger(FlywayDB.class);

    public void check() {
        logger.info("Start database migration");

        final InitialContext cxt;
        final DataSource dataSource;

        try {
            cxt = new InitialContext();
            dataSource = (DataSource) cxt.lookup("java:/comp/env/jdbc/cosmic");
        } catch (final NamingException e) {
            logger.error(e.getMessage(), e);
            throw new CloudRuntimeException(e.getMessage(), e);
        }

        final FluentConfiguration flywayConfig = new FluentConfiguration()
                .encoding("UTF-8")
                .dataSource(dataSource)
                .table("schema_version");
        final Flyway flyway = new Flyway(flywayConfig);

        try {
            flyway.migrate();
        } catch (final FlywayException e) {
            logger.error(e.getMessage(), e);
            throw new CloudRuntimeException(e.getMessage(), e);
        }

        logger.info("Database migration successful");
    }
}
