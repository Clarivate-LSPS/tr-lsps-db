package com.thomsonreuters.lsps.db.loader

import com.thomsonreuters.lsps.db.core.Database
import com.thomsonreuters.lsps.db.core.DatabaseUtils
import groovy.sql.Sql
import org.postgresql.PGConnection
/**
 * Created by bondarev on 4/8/14.
 */
abstract class DataLoader {
    String tableName
    Collection<String> columnNames

    static long start(Database database, String tableName, Collection<String> columnNames, Closure block) {
        database.withSql { sql ->
            start(sql, tableName, columnNames, block)
        }
    }

    static long start(Sql sql, String tableName, Collection<String> columnNames, Closure block) {
        columnNames = columnNames.collect { !it.startsWith('"') ? "\"${it}\"" : it }
        long result = 0
        sql.withTransaction {
            def originalConnection = DatabaseUtils.getOriginalConnection(sql)
            DataLoader dataLoader
            // check Postgres database
            if (DatabaseUtils.isPGConnection(originalConnection)) {
                columnNames = columnNames*.toLowerCase()
                dataLoader = new PGCopyDataLoader(connection: originalConnection as PGConnection, tableName: tableName, columnNames: columnNames)
            } else {
                dataLoader = new SqlDataLoader(sql: sql, tableName: tableName, columnNames: columnNames)
            }

            result = dataLoader.withBatch(block)
        }
        return result
    }

    abstract long withBatch(Closure block);
}
