package org.tallison.tika.eval;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.tika.Tika;
import org.apache.tika.eval.io.DBWriter;
import org.apache.tika.io.IOExceptionWithCause;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This reads through a directory and outputs a tab-delimited file
 * relative path \t sha256 digest \t mime
 *
 * If you want to write to a sqlite db, substitute a jdbc string for the writer path
 *
 * java -cp eval-addons.jar org.tallison.tika.eval.MimeAndDigester /data/docs jdbc:sqlite:/user/home/data/sqlite.db 15
 */
public class MimeAndDigester {
    private static final Path POISON_PATH = Paths.get("");
    private static final int ROW_QUEUE_LENGTH = 10000;
    private static final int PATH_QUEUE_LENGTH = 10000;
    private static final String TIKA_DETECT_EXCEPTION = "tika-detect-exception";
    private static final String FILE_DETECT_EXCEPTION = "file-detect-exception";
    private static final Row POISON_ROW = new Row();

    private static final Row HEADER = new Row();
    static {
        HEADER.cells.add("Path");
        HEADER.cells.add("Sha256");
        HEADER.cells.add("TikaMime");
        HEADER.cells.add("FileMime");
    }

    public static void main(String[] args) throws Exception {
        Path root = Paths.get(args[0]);
        String writerPath = args[1];
        int numConsumers = Integer.parseInt(args[2]);

        ExecutorService executorService = Executors.newFixedThreadPool(numConsumers+2);
        ExecutorCompletionService completionService = new ExecutorCompletionService(executorService);

        ArrayBlockingQueue<Path> paths = new ArrayBlockingQueue<>(PATH_QUEUE_LENGTH);
        ArrayBlockingQueue<Row> rows = new ArrayBlockingQueue<>(ROW_QUEUE_LENGTH);

        completionService.submit(new PathAdder(paths, numConsumers, root));
        completionService.submit(new MimeDigestWriter(rows, numConsumers, writerPath));

        Tika tika = new Tika();
        for (int i = 0; i < numConsumers; i++) {
            completionService.submit(new MimeDigesterWorker(root, paths, rows, tika));
        }

        int completed = 0;
        int totalThreads = numConsumers+2;
        while (completed < totalThreads) {
            Future<Integer> future = completionService.take();//blocking
            future.get();
            completed++;
        }
        executorService.shutdownNow();
    }

    private static final class PathAdder implements Callable<Integer> {
        private final long absoluteWaitSeconds = 600;
        private final ArrayBlockingQueue<Path> paths;
        private final int numConsumers;
        private final Path root;

        private PathAdder(ArrayBlockingQueue<Path> paths, int numConsumers, Path root) {
            this.paths = paths;
            this.numConsumers = numConsumers;
            this.root = root;
        }

        @Override
        public Integer call() throws Exception {
            processDir(root);

            for (int i = 0; i < numConsumers; i++) {
                boolean offered = paths.offer(POISON_PATH, absoluteWaitSeconds, TimeUnit.SECONDS);
                if (! offered) {
                    throw new RuntimeException("Couldn't add poison after absoluteWaitSeconds");
                }
            }
            return 1;
        }

        private void processDir(Path dir) throws InterruptedException {
            for (File f : dir.toFile().listFiles()) {
                if (f.isDirectory()) {
                    processDir(f.toPath());
                } else if (f.isFile()) {
                    boolean offered = paths.offer(f.toPath(), absoluteWaitSeconds, TimeUnit.SECONDS);
                    if (!offered) {
                        throw new RuntimeException("couldn't add file after absoluteWaitSeconds");
                    }
                }
            }
        }
    }

    private static final class MimeDigesterWorker implements Callable<Integer> {
        private final long absoluteWaitSeconds = 600;
        private final Path root;
        private final ArrayBlockingQueue<Path> paths;
        private final ArrayBlockingQueue<Row> rows;
        private final Tika tika;

        private MimeDigesterWorker(Path root,
                                   ArrayBlockingQueue<Path> paths,
                                   ArrayBlockingQueue<Row> rows,
                                   Tika tika) {
            this.root = root;
            this.paths = paths;
            this.rows = rows;
            this.tika = tika;
        }


        @Override
        public Integer call() throws Exception {
            while (true) {
                Path path = paths.poll(absoluteWaitSeconds, TimeUnit.SECONDS);
                if (path == null) {
                    System.err.println("exceeded absoluteWaitSeconds");
                    return 0;
                }
                if (path == POISON_PATH) {
                    boolean offered = rows.offer(POISON_ROW, absoluteWaitSeconds, TimeUnit.SECONDS);
                    if (! offered) {
                        throw new RuntimeException("Exceeded absolute wait trying to add poison row");
                    }
                    return 1;
                }
                processPath(path);
            }
        }

        private void processPath(Path path) throws IOException, InterruptedException {
            Row row = new Row();
            row.cells.add(root.relativize(path).toString());

            try (InputStream is = new BufferedInputStream(Files.newInputStream(path))) {
                row.cells.add(DigestUtils.sha256Hex(is));
            }
            String tikaMime = null;
            try {
                tikaMime = tika.detect(path);
            } catch (IOException e) {
                tikaMime = TIKA_DETECT_EXCEPTION;
            }
            row.cells.add(tikaMime);
            String fileMime = null;
            try {
                fileMime = FileMime.detect(path);
            } catch (IOException e) {
                fileMime = FILE_DETECT_EXCEPTION;
            }
            row.cells.add(fileMime);
            boolean offered = rows.offer(row, absoluteWaitSeconds, TimeUnit.SECONDS);
            if (! offered) {
                throw new RuntimeException("failed to add row");
            }
        }
    }


    private static final class MimeDigestWriter implements Callable<Integer> {

        private final long absoluteWaitSeconds = 600;
        private final ArrayBlockingQueue<Row> rows;
        private final int numConsumers;
        private final TableWriter writer;

        MimeDigestWriter(ArrayBlockingQueue<Row> rows, int numConsumers, String writerPath) throws IOException {
            this.rows = rows;
            this.numConsumers = numConsumers;
            this.writer = TableWriter.getWriter(writerPath);
        }


        @Override
        public Integer call() throws Exception {
            int poisonRows = 0;
            while (true) {

                Row r = rows.poll(absoluteWaitSeconds, TimeUnit.SECONDS);
                if (r == null) {
                    System.err.println("exceeded absoluteWaitSeconds");
                    return 0;
                }
                if (r == POISON_ROW) {
                    poisonRows++;
                    if (poisonRows >= numConsumers) {
                        writer.close();
                        return 1;
                    }
                } else {
                    writer.writeRow(r);
                }
            }
        }
    }

    private static abstract class TableWriter implements Closeable {

        public static TableWriter getWriter(String path) throws IOException {
            if (path.startsWith("jdbc:")) {
                return new DBTableWriter(path);
            } else {
                return new TSVTableWriter(Paths.get(path));
            }
        }

        private int rows = 0;
        private static long STARTED = System.currentTimeMillis();

        public void writeRow(Row row) throws IOException {
            if (++rows % 1000 == 0) {
                long elapsed = System.currentTimeMillis() - STARTED;
                System.out.println("wrote "+rows+ " files in "+elapsed+" ms");
            }
        }

        public void close() throws IOException {
            long elapsed = System.currentTimeMillis() - STARTED;
            System.out.println("Completed. Wrote "+rows+ " files in "+elapsed+" ms");
        }

    }

    private static class DBTableWriter extends TableWriter {
        private final Connection connection;
        private final PreparedStatement insert;
        int rows = 0;
        public DBTableWriter(String path) throws IOException {

            try {
                if (path.startsWith("jdbc:sqlite:")) {
                    Class.forName("org.sqlite.JDBC");
                } else if (path.startsWith("jdbc:h2:")) {
                    Class.forName("org.h2.Driver");
                } else if (path.startsWith("jdbc:postgresql:")) {
                    Class.forName("org.postgresql.Driver");
                }
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
            try {
                this.connection = DriverManager.getConnection(path);
                if (path.startsWith("jdbc:sqlite")) {
                    String sql = "PRAGMA synchronous = OFF";
                    connection.createStatement().execute(sql);
                    //sql = "PRAGMA journal_mode = MEMORY";
                    //connection.createStatement().execute(sql);
                }
                connection.setAutoCommit(false);
                String sql = "drop table if exists digest_mimes";
                connection.createStatement().execute(sql);
                sql = "create table digest_mimes " +
                        "(path varchar(1024), sha256 varchar(64), tika_mime varchar(1024), " +
                        "file_mime varchar(1024));";
                connection.createStatement().execute(sql);
                sql = "insert into digest_mimes values (?, ?, ?, ?)";
                insert = connection.prepareStatement(sql);
            } catch (SQLException e) {
                throw new IOExceptionWithCause(e);
            }

        }

        @Override
        public void writeRow(Row row) throws IOException {
            try {
                for (int i = 0; i < row.cells.size(); i++) {
                    insert.setString(i + 1, row.cells.get(i));
                }
                insert.addBatch();
                rows++;
                if (rows % 10000 == 0) {
                    insert.executeBatch();
                    connection.commit();
                }
                super.writeRow(row);
            } catch (SQLException e) {
                throw new IOExceptionWithCause(e);
            }
        }

        @Override
        public void close() throws IOException {
            try {
                insert.executeBatch();
                insert.close();
                connection.commit();
                connection.close();
            } catch (SQLException e) {
                throw new IOExceptionWithCause(e);
            } finally {
                super.close();
            }
        }
    }

    private static class TSVTableWriter extends TableWriter {

        final BufferedWriter writer;

        TSVTableWriter(Path path) throws IOException {
            writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8);
            writeRow(HEADER);
        }

        @Override
        public void writeRow(Row r) throws IOException {
            StringBuilder sb = new StringBuilder();
            int i = 0;
            for (String cell : r.cells) {
                if (i++ > 0) {
                    sb.append("\t");
                }
                sb.append(cell.replaceAll("[\t\r\n]", " "));
            }
            sb.append("\n");
            writer.write(sb.toString());
            super.writeRow(r);
        }


        @Override
        public void close() throws IOException {
            writer.flush();
            writer.close();
            super.close();
        }
    }

    private static class Row {
        List<String> cells = new ArrayList<>();
    }
}
