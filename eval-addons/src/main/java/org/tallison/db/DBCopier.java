/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.tallison.db;

import org.h2.util.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * TODO: Abstract the db specific stuff (get tables, etc) into this class
 */
public abstract class DBCopier {

    private static final int BATCH_SIZE = 100000;

    final Connection from;
    final Connection to;
    final String toSchema;//can be null

    public DBCopier(Connection from, Connection to, String toSchema) {
        this.from = from;
        this.to = to;
        this.toSchema = toSchema;
    }
    public abstract List<String> getTables() throws SQLException;

    abstract String getPrimaryKey(String tableName) throws SQLException;
    abstract String getColumnTypeName(int type, String columnName);

    void execute() throws SQLException {
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

    void insert(String table) throws SQLException {

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

    PreparedStatement createInsert(String table) throws SQLException {
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

    void createTable(String table) throws SQLException {
        try(Statement st = to.createStatement()) {
            st.execute("DROP TABLE IF EXISTS "+getSchemaTable(table));
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

    void updateInsert(String tableName, int[] targetTypes, ResultSet rs, PreparedStatement insert, int rowCount) throws SQLException {

        for (int i = 0; i < targetTypes.length; i++) {
            updateCell(i + 1, i+1, targetTypes[i], rs, insert);
        }
    }
    String getSchemaTable(String table) {
        if (!StringUtils.isNullOrEmpty(toSchema)) {
            return toSchema+"."+table;
        }
        return table;
    }

    void updateCell(int srcColumnOffset, int targColumnOffset, int targetColumnType, ResultSet rs, PreparedStatement insert) throws SQLException {
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

    private void debug(ResultSet rs) throws SQLException {
        for (int i = 1; i < rs.getMetaData().getColumnCount(); i++) {
            System.out.println(i + " : "+rs.getMetaData().getColumnName(i) + " : "+rs.getString(i));
        }
    }
}
