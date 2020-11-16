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
package org.tallison.batchlite.writer;

import org.apache.tika.io.IOExceptionWithCause;
import org.tallison.batchlite.FileProcessResult;
import org.tallison.batchlite.MetadataWriter;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

//e.g. /data/docs output jdbc:h2:file:/home/tallison/Desktop/h2_results:file_metadata 10

public class JDBCMetadataWriter implements MetadataWriter {

    private static final long MAX_POLL_SECONDS = 600;
    private static final int MAX_STREAM_LENGTH = 20000;

    private final Connection connection;
    private final PreparedStatement insert;
    private final ArrayBlockingQueue<PathResultPair> queue = new ArrayBlockingQueue<>(1000);
    private final ExecutorService executorService;
    private final ExecutorCompletionService<Integer> executorCompletionService;
    private final WriterThread writerThread;

    JDBCMetadataWriter(String jdbcString) throws IOException {
        int tableIndex = jdbcString.lastIndexOf(":");
        if (tableIndex < 0) {
            throw new RuntimeException("must specify table name after :");
        }
        String table = jdbcString.substring(tableIndex+1);
        jdbcString = jdbcString.substring(0, tableIndex);
        String sql = "insert into "+table+" values (?,?,?,?,?," +
                "?,?,?,?,?);";
        try {
            connection = DriverManager.getConnection(jdbcString);
            createTable(connection, table);
            insert = connection.prepareStatement(sql);
        } catch (SQLException e) {
            throw new IOExceptionWithCause(e);
        }
        executorService = Executors.newFixedThreadPool(1);
        executorCompletionService = new ExecutorCompletionService<>(executorService);
        this.writerThread = new WriterThread(queue, insert);
        executorCompletionService.submit(this.writerThread);
    }

    private static void createTable(Connection connection, String table) throws SQLException {
        String sql = "create table "+table+" ("+
                "path varchar(5000) primary key,"+
                "exit_value integer," +
                "timeout boolean,"+
                "process_time_ms BIGINT,"+
                "stderr varchar("+MAX_STREAM_LENGTH+"),"+
                "stderr_length bigint,"+
                "stderr_truncated boolean,"+
                "stdout varchar("+MAX_STREAM_LENGTH+"),"+
                "stdout_length bigint,"+
                "stdout_truncated boolean)";
        connection.createStatement().execute(sql);
    }

    @Override
    public void write(String relPath, FileProcessResult result) throws IOException {
        try {
            boolean offered = queue.offer(new PathResultPair(relPath, result), MAX_POLL_SECONDS, TimeUnit.SECONDS);
            if (! offered) {
                throw new IOExceptionWithCause(
                        new TimeoutException("exceeded "+MAX_POLL_SECONDS
                                + " seconds"));
            }
        } catch (InterruptedException e) {
            throw new IOExceptionWithCause(e);
        }
    }

    @Override
    public int getRecordsWritten() {
        return writerThread.getRecordsWritten();
    }

    @Override
    public void close() throws IOException {
        try {
            boolean offered = queue.offer(PathResultPair.POISON, MAX_POLL_SECONDS, TimeUnit.SECONDS);
            if (! offered) {
                throw new IOExceptionWithCause(
                        new TimeoutException("exceeded "+MAX_POLL_SECONDS
                                + " seconds"));
            }
        } catch (InterruptedException e) {
            throw new IOExceptionWithCause(e);
        }
        Future<Integer> future = null;
        try {
            future = executorCompletionService.poll(MAX_POLL_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new IOExceptionWithCause(e);
        }
        if (future == null) {
            throw new IOExceptionWithCause(
                    new TimeoutException("exceeded "+MAX_POLL_SECONDS
            + " seconds"));
        }
        try {
            future.get();
        } catch (InterruptedException|ExecutionException e) {
            throw new IOExceptionWithCause(e);
        } finally {
            executorService.shutdownNow();
        }
        try {
            insert.executeBatch();
            insert.close();
            connection.commit();
            connection.close();
        } catch (SQLException e) {
            throw new IOExceptionWithCause(e);
        }
    }

    private static class WriterThread implements Callable<Integer> {
        private ArrayBlockingQueue<PathResultPair> queue;
        private final PreparedStatement insert;
        private int recordsWritten = 0;

        WriterThread(ArrayBlockingQueue<PathResultPair> queue, PreparedStatement insert) {
            this.queue = queue;
            this.insert = insert;
        }

        @Override
        public Integer call() throws Exception {
            while (true) {
                PathResultPair pair = queue.poll(MAX_POLL_SECONDS, TimeUnit.SECONDS);
                if (pair == null) {
                    throw new TimeoutException("waited longer than " + MAX_POLL_SECONDS
                            + " seconds");
                }
                if (pair == PathResultPair.POISON) {
                    return 1;
                }
                int i = 0;
                FileProcessResult result = pair.getResult();
                insert.setString(++i, pair.getRelPath());
                insert.setInt(++i, result.getExitValue());
                insert.setBoolean(++i, result.isTimeout());
                insert.setLong(++i, result.getProcessTimeMillis());
                insert.setString(++i, result.getStderr());
                insert.setLong(++i, result.getStderrLength());
                insert.setBoolean(++i, result.isStderrTruncated());
                insert.setString(++i, result.getStdout());
                insert.setLong(++i, result.getStdoutLength());
                insert.setBoolean(++i, result.isStdoutTruncated());
                insert.addBatch();
                recordsWritten++;
                if (recordsWritten % 1000 == 0) {
                    System.out.println("processed: "+recordsWritten);
                }
                if (recordsWritten % 10000 == 0) {
                    insert.executeBatch();
                }
            }
        }

        int getRecordsWritten() {
            return recordsWritten;
        }
    }
}
