package org.tallison.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.h2.util.StringUtils;

/**
 * WARNING: This class will delete all tables in your 'public' schema!!!
 *
 * This is a quick hack to copy contents from tika-eval's h2 db into postgresql.
 * It is not a general purpose db copier!!!
 *
 * This does not handle exceptions nor does it log anything aside from println :)
 * The idea is that an exception should be a stop the world kind of thing.
 *
 * The commandline should look like the following.  Make sure to turn
 * on LAZY_QUERY_EXECUTION in the h2 db.
 *
 * jdbc:h2:file:C:/data/tika_1_18V1_19-rc1;LAZY_QUERY_EXECUTION=1
 * jdbc:postgresql://localhost/tika-eval?user=XXX&password=YYY
 */
public class H2ToPostgres extends DBCopier {

    private final String pgUser;
    public H2ToPostgres(Connection from, Connection to, String toSchema, String pgUser) {
        super(from, to, toSchema);
        this.pgUser = pgUser;
    }

    @Override
    public List<String> getTables() throws SQLException {
        String sql = "SHOW TABLES";
        List<String> tables = new ArrayList<>();
        ResultSet rs = from.createStatement().executeQuery(sql);
        while (rs.next()) {
            tables.add(rs.getString(1));
        }
        return tables;
    }

    public static void main(String[] args) throws Exception {
        Class.forName("org.h2.Driver");
        Class.forName("org.postgresql.Driver");

        Connection from = DriverManager.getConnection(args[0]);
        Connection to = DriverManager.getConnection(args[1]);
        String toSchema = (args.length > 2) ? args[2] : "";
        String pgUser = null;//necessary if creating a new schema
        if (args.length > 2) {
            Matcher m = Pattern.compile("user=([^&]+)").matcher(args[1]);
            if (m.find()) {
                pgUser = m.group(1);
            }
        }
        H2ToPostgres copier = new H2ToPostgres(from, to, toSchema, pgUser);
        copier.execute();
        from.close();
        to.close();
    }


    @Override
    void createTable(String table) throws SQLException {
        try(Statement st = to.createStatement()) {
            if (!StringUtils.isNullOrEmpty(toSchema)) {
                st.execute("CREATE SCHEMA IF NOT EXISTS " + toSchema + " AUTHORIZATION " + pgUser);
            }
        }
        super.createTable(table);
    }

/*
    private void deleteTablesInTo() throws SQLException {
        List<String> tables = new ArrayList<>();
        String sql = "SELECT tablename FROM pg_catalog.pg_tables where schemaname='public'";
        try (Statement st = to.createStatement()) {
            try (ResultSet rs = st.executeQuery(sql)) {
                while (rs.next()) {
                    tables.add(rs.getString(1));
                }
            }
        }

        for (String table : tables) {
            sql = "drop table if exists "+table;
            try (Statement st = to.createStatement()) {
                st.execute(sql);
            }
        }
    }
*/



    @Override
    String getColumnTypeName(int type, String columnName) {
        switch (type) {
            case (Types.DOUBLE) :
                return "real";

            default:
                return columnName;
        }
    }

    @Override
    String getPrimaryKey(String tableName) throws SQLException {
        try (Statement st = from.createStatement()) {
            String sql = "select COLUMN_NAME from information_schema.indexes where table_name ='"+tableName+"';";
            try (ResultSet rs = st.executeQuery(sql)) {
                while (rs.next()) {
                    String colName = rs.getString(1);
                    return colName;
                }
            }
        }
        return null;
    }
}
