package com.thomsonreuters.lsps.db.core

import groovy.sql.Sql
import org.postgresql.PGConnection

import java.sql.Connection

/**
 * Date: 29-Feb-16
 * Time: 12:27
 */
class DatabaseUtils {
    final static Class PGConnectionCls

    static {
        try {
            PGConnectionCls = PGConnection
        } catch (NoClassDefFoundError ignored) {
            PGConnectionCls = null
        }
    }

    static Connection getOriginalConnection(Sql sql) {
        sql.connection.metaData.connection
    }

    static boolean isPGConnection(Connection connection) {
        PGConnectionCls?.isInstance(connection)
    }
}
