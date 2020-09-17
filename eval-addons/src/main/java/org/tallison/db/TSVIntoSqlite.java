package org.tallison.db;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.zip.GZIPInputStream;

public class TSVIntoSqlite {

    public static void main(String[] args) throws Exception {
        Class.forName("org.sqlite.JDBC");
        Connection sqlite = DriverManager.getConnection(args[0]);
        sqlite.setAutoCommit(false);
        BufferedReader reader = getReader(args[1]);
        String table = "all_files";
        String sql = "drop table if exists "+table;
        sqlite.createStatement().execute(sql);

        sql = "create table "+table+
                " (file varchar(1024) primary key)";//, mime varchar(1024))";
        sqlite.createStatement().execute(sql);

        String line = reader.readLine();
        //header
        //line = reader.readLine();
        sql = "insert into "+table+ " values (?)";//,?)";
        PreparedStatement ps = sqlite.prepareStatement(sql);
        int batchCount = 0;
        int total = 0;
        while (line != null) {
            String[] cols = line.split("\t");
            if (cols.length < 1) {
                System.out.println("line "+line);
            } else {
//                ps.setString(1, cols[0]);
  //              ps.setString(2, cols[1]);
                ps.setString(1, line.trim());
                ps.addBatch();
            }
            if (batchCount++ > 10000) {
                System.out.println("inserting "+total);
                ps.executeBatch();
                batchCount = 0;
            }
            total++;
            line = reader.readLine();
        }
        ps.executeBatch();
        sqlite.commit();
    }

    private static BufferedReader getReader(String path) throws IOException {
        Path p = Paths.get(path);
        InputStream is = (path.endsWith("gz")) ?
                new GZIPInputStream(Files.newInputStream(p)) :
                Files.newInputStream(p);
        return new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
    }
}
