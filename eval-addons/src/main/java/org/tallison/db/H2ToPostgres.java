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

    private static final int BATCH_SIZE = 100000;
    private final String toSchema;
    private final String pgUser;
    public H2ToPostgres(Connection from, Connection to, String toSchema, String pgUser) {
        super(from, to);
        this.toSchema = toSchema;
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


    private void execute() throws SQLException {
        to.setAutoCommit(false);
        //deleteTablesInTo();
        to.commit();
        for (String table : getTables()) {
            createTable(table);
            to.commit();
            insert(table);
            to.commit();
        }
        to.commit();
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
    private void insert(String table) throws SQLException {

        PreparedStatement insert = createInsert(table);

        String selectStar = "select * from "+table;
        try (Statement st = from.createStatement()) {
            System.out.println("about to do big select star");
            try (ResultSet rs = st.executeQuery(selectStar)) {
                System.out.println("finished big select star");
                int[] targetTypes = new int[rs.getMetaData().getColumnCount()];
                for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
                    targetTypes[i] = rs.getMetaData().getColumnType(i + 1);
                    System.out.println(rs.getMetaData().getColumnType(i + 1) + " ; " +
                            rs.getMetaData().getColumnTypeName(i + 1));
                }
                int rows = 0;
                Instant started = Instant.now();
                while (rs.next()) {
                    updateInsert(table, targetTypes, rs, insert, rows++);
                    insert.addBatch();
                    if (rows % BATCH_SIZE == 0) {
                        try {
                            insert.executeBatch();
                            insert.clearParameters();
                            System.out.println("inserted " + rows + " rows into " + table
                                    + " in "+(Duration.between(started, Instant.now()).toMillis() + " milliseconds"));
                        } catch (SQLException e) {
                            debug(rs);
                            throw e;
                        }
                    }
                }
                insert.executeBatch();
                insert.clearParameters();
                insert.close();
            }
        }
    }

    private void debug(ResultSet rs) throws SQLException {
        for (int i = 1; i < rs.getMetaData().getColumnCount(); i++) {
            System.out.println(i + " : "+rs.getMetaData().getColumnName(i) + " : "+rs.getString(i));
        }
    }

    private void updateInsert(String tableName, int[] targetTypes, ResultSet rs, PreparedStatement insert, int rowCount) throws SQLException {

        for (int i = 0; i < targetTypes.length; i++) {
            updateCell(i + 1, i+1, targetTypes[i], rs, insert);
        }
    }

    private void updateCell(int srcColumnOffset, int targColumnOffset, int targetColumnType, ResultSet rs, PreparedStatement insert) throws SQLException {
        switch (targetColumnType) {
            case Types.BOOLEAN:
                boolean boolVal = rs.getBoolean(srcColumnOffset);
                if (rs.wasNull()) {
                    insert.setNull(targColumnOffset, targetColumnType);
                } else {
                    insert.setBoolean(targColumnOffset, boolVal);
                }
                break;
            case Types.DATE:
                java.sql.Date dateVal = rs.getDate(srcColumnOffset);
                if (dateVal == null || rs.wasNull()) {
                    insert.setNull(targColumnOffset, targetColumnType);
                } else {
                    insert.setDate(targColumnOffset, dateVal);
                }
                break;
            case Types.TIMESTAMP:
                Timestamp tsVal = rs.getTimestamp(srcColumnOffset);
                if (tsVal == null || rs.wasNull()) {
                    insert.setNull(targColumnOffset, targetColumnType);
                } else {
                    insert.setTimestamp(targColumnOffset, tsVal);
                }
                break;
            case Types.INTEGER:
                int intVal = rs.getInt(srcColumnOffset);
                if (rs.wasNull()) {
                    insert.setNull(targColumnOffset, targetColumnType);
                } else {
                    insert.setInt(targColumnOffset, intVal);
                }
                break;
            case Types.BIGINT:
                long longVal = rs.getLong(srcColumnOffset);
                if (rs.wasNull()) {
                    insert.setNull(targColumnOffset, targetColumnType);
                } else {
                    insert.setLong(targColumnOffset, longVal);
                }
                break;
            case Types.FLOAT:
                float fltVal = rs.getFloat(srcColumnOffset);
                if (rs.wasNull()) {
                    insert.setNull(targColumnOffset, targetColumnType);
                } else {
                    insert.setFloat(targColumnOffset, fltVal);
                }
                break;
            case Types.DOUBLE:
                double dblVal = rs.getDouble(srcColumnOffset);
                if (rs.wasNull()) {
                    insert.setNull(targColumnOffset, targetColumnType);
                } else {
                    insert.setDouble(targColumnOffset, dblVal);
                }
                break;
            case Types.CHAR:
            case Types.NCHAR:
                String sChar = rs.getString(srcColumnOffset);
                if (sChar == null || rs.wasNull()) {
                    insert.setNull(targColumnOffset, Types.CHAR);
                } else {
                    //pg can't handle \u0000
                    sChar = sChar.replaceAll("\u0000", " ");
                    insert.setString(targColumnOffset, sChar);
                }
                break;
            case Types.VARCHAR:
            case Types.NVARCHAR:
                String sVarchar = rs.getString(srcColumnOffset);
                if (sVarchar == null || rs.wasNull()) {
                    insert.setNull(targColumnOffset, Types.VARCHAR);
                } else {
                    //pg can't handle \u0000
                    sVarchar = sVarchar.replaceAll("\u0000", " ");
                    insert.setString(targColumnOffset, sVarchar);
                }
                break;
            default:
                throw new RuntimeException("I regret I don't yet know how to process this type of column: "
                        + targetColumnType);
        }

    }


    private PreparedStatement createInsert(String table) throws SQLException {
        String sql = "select * from "+table+" limit 1";
        try (Statement st = from.createStatement()) {
            try (ResultSet rs = st.executeQuery(sql)) {
                StringBuilder sb = new StringBuilder();
                sb.append("INSERT INTO ").append(getSchemaTable(table)).append("(");
                StringBuilder q = new StringBuilder();
                q.append("(");
                for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                    sb.append(rs.getMetaData().getColumnName(i));
                    q.append("?");
                    if (i != rs.getMetaData().getColumnCount()) {
                        sb.append(", ");
                        q.append(",");
                    }
                }
                q.append(")");
                sb.append(") VALUES ").append(q);
                System.out.println(sb.toString());
                return to.prepareStatement(sb.toString());
            }
        }
    }

    private void createTable(String table) throws SQLException {
        try(Statement st = to.createStatement()) {
            st.execute("DROP TABLE IF EXISTS "+getSchemaTable(table));
            if (! StringUtils.isNullOrEmpty(toSchema)) {
                st.execute("CREATE SCHEMA IF NOT EXISTS " + toSchema + " AUTHORIZATION "+pgUser);
            }
        }

        String primaryKeyColumn = getPrimaryKey(table);

        String select = "SELECT * FROM "+table+" LIMIT 1";
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(getSchemaTable(table)).append("(");
        try (Statement st = from.createStatement()) {
            try (ResultSet resultSet = st.executeQuery(select)) {
                System.out.println("TABLE: " + getSchemaTable(table));
                ResultSetMetaData metaData = resultSet.getMetaData();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    sb.append(metaData.getColumnName(i)).append(" ");
                    sb.append(getColumnTypeName(metaData.getColumnType(i), metaData.getColumnTypeName(i)));
                    if (metaData.getColumnType(i) == Types.CHAR ||
                            metaData.getColumnType(i) == Types.NCHAR ||
                            metaData.getColumnType(i) == Types.VARCHAR ||
                            metaData.getColumnType(i) == Types.NVARCHAR) {
                        sb.append("(").append(metaData.getPrecision(i)).append(")");
                    }
                    if (primaryKeyColumn != null && primaryKeyColumn.equalsIgnoreCase(metaData.getColumnName(i))) {
                        sb.append(" PRIMARY KEY");
                    }
                    if (i != metaData.getColumnCount()) {
                        sb.append(",\n");
                    }
                }
                sb.append(")");
            }
        }
        String createTableSQL = sb.toString();
        try (Statement st = to.createStatement()) {
            System.out.println(createTableSQL);
            st.execute(createTableSQL);
        }
    }

    private String getSchemaTable(String table) {
        if (!StringUtils.isNullOrEmpty(toSchema)) {
            return toSchema+"."+table;
        }
        return table;
    }

    private String getColumnTypeName(int type, String columnName) {
        switch (type) {
            case (Types.DOUBLE) :
                return "real";

            default:
                return columnName;
        }
    }

    private String getPrimaryKey(String tableName) throws SQLException {
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
