package com.thomsonreuters.lsps.db.loader

import groovy.transform.CompileStatic
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVPrinter
import org.postgresql.PGConnection
import org.postgresql.copy.PGCopyOutputStream

/**
 * Created by bondarev on 4/21/14.
 */
@CompileStatic
class PGCopyDataLoader extends DataLoader {
    PGConnection connection

    private static class BatchWriter {
        private CSVPrinter printer

        BatchWriter(Appendable out) {
            printer = new CSVPrinter(out, CSVFormat.TDF.withRecordSeparator(System.getProperty("line.separator")))
        }

        @SuppressWarnings("GroovyUnusedDeclaration")
        def addBatch(Object[] data) {
            addBatch(data as List)
        }

        def addBatch(List data) {
            for (def value : data) {
                if (value instanceof Boolean) {
                    value = value ? 't' : 'f'
                }
                printer.print(value)
            }
            printer.println()
        }
    }

    @Override
    long withBatch(Closure block) {
        String command = "COPY ${tableName}"
        if (columnNames) {
            command += "(${columnNames.join(', ')})"
        }
        command += " FROM STDIN WITH (FORMAT CSV, DELIMITER '\t')"
        def out = new PGCopyOutputStream(connection, command)
        def printer = new OutputStreamWriter(out)
        try {
            block.call(new BatchWriter(printer))
            printer.flush()
        } catch (Throwable ex){
            try {
                out.cancelCopy()
            } catch (Exception ignored) {
            }
            throw ex;
        }
        return out.endCopy()
    }
}
