import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 *
 * @author Martin Schaeler martin.schaeler@plus.ac.at
 *
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
 */


public class Blocking {
    public static int num_taks = 30;
    public static int num_queries_per_task = 3;

    Random rand = new Random(123456);             // only used to load the data
    int num_tuple = 50000;
    double max_value = 50.0f;

    AtomicInteger tasks_finished = new AtomicInteger(0);  // committed transactions
    AtomicInteger deadlocks = new AtomicInteger(0);       // deadlock retries


    static final java.io.PrintStream OUT = new java.io.PrintStream(
            new java.io.BufferedOutputStream(new java.io.FileOutputStream(java.io.FileDescriptor.out), 1 << 16), false);


    Connection openConnection() throws SQLException {
        Connection c = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);


        c.setAutoCommit(false);
        return c;
    }

    // Measurement: running time per transaction type (from the start of the first attempt until the commit)
    static final String[] TYPES = {"point read (60%)", "insert (10%)", "full scan (10%)", "write (20%)"};
    final LongAdder[] typeCount = {new LongAdder(), new LongAdder(), new LongAdder(), new LongAdder()};
    final LongAdder[] typeNanos = {new LongAdder(), new LongAdder(), new LongAdder(), new LongAdder()};

    void executeSQL(String sql, Connection con) {
        try (Statement stmt = con.createStatement()) {
            stmt.execute(sql);
            System.out.println("Executing " + sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // Loads 50,000 tuples. Not part of the measured time.
    void cerateDB() {
        try {
            Connection con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
            executeSQL("DROP TABLE IF EXISTS blocking_data;", con);  // also removes tuples inserted by the last run
            executeSQL("CREATE TABLE blocking_data ( data_id integer primary key, data_value float, modified_by integer);", con);


            executeSQL("DO $$ BEGIN EXECUTE format('ALTER DATABASE %I SET synchronous_commit = off', current_database()); END $$;", con);

            con.setAutoCommit(false);
            PreparedStatement ps = con.prepareStatement("INSERT INTO blocking_data VALUES (?, ?, ?)");
            for (int i = 0; i < num_tuple; i++) {
                ps.setInt(1, i);
                ps.setDouble(2, rand.nextDouble() * max_value);
                ps.setInt(3, 0);
                ps.addBatch();
            }
            int[] results = ps.executeBatch();
            con.commit();
            ps.close();
            con.close();
            System.out.println("Loaded " + results.length + " tuples");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws InterruptedException {

        int workers = args.length > 0 ? Integer.parseInt(args[0]) : 4;
        int seconds = args.length > 1 ? Integer.parseInt(args[1]) : 20;

        Blocking b = new Blocking();
        b.cerateDB();

        long durationNanos = TimeUnit.SECONDS.toNanos(seconds);
        AtomicInteger nextTaskId = new AtomicInteger(0);
        ExecutorService executor = Executors.newFixedThreadPool(workers);
        long start = System.nanoTime();

        for (int w = 0; w < workers; w++) {
            executor.execute(() -> {
                while (System.nanoTime() - start < durationNanos) {
                    b.new Blocker(nextTaskId.getAndIncrement()).run();
                }
            });
        }
        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

        // throughput = committed transactions per second
        double elapsed = (System.nanoTime() - start) / 1e9;
        OUT.flush();                         // write out the remaining buffered lines

        //reset synchronous_commit for the database, so it does not stay off after the benchmark
        try (Connection c = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL)) {
            b.executeSQL("DO $$ BEGIN EXECUTE format('ALTER DATABASE %I RESET synchronous_commit', current_database()); END $$;", c);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        int committed = b.tasks_finished.get();
        System.out.println();
        System.out.println("Worker threads: " + workers);
        System.out.println("Transactions started: " + nextTaskId.get());
        System.out.println("Transactions committed: " + committed);
        System.out.println("Deadlock retries: " + b.deadlocks.get());
        System.out.printf("Elapsed time: %.3f seconds%n", elapsed);
        System.out.printf("Throughput: %.2f commits/second%n", committed / elapsed);

        // where does the transaction running time go?
        long all = 0;
        for (LongAdder n : b.typeNanos) all += n.sum();
        System.out.println();
        System.out.printf("%-18s %10s %14s %16s%n", "Transaction type", "Count", "Avg time (ms)", "Share of time");
        for (int k = 0; k < TYPES.length; k++) {
            long c = b.typeCount[k].sum(), n = b.typeNanos[k].sum();
            System.out.printf("%-18s %10d %14.3f %15.1f%%%n", TYPES[k], c,
                    c == 0 ? 0.0 : n / 1e6 / c, all == 0 ? 0.0 : 100.0 * n / all);
        }
    }

    /** One transaction (a Blocker instance). */
    public class Blocker implements Runnable {
        final int task;
        //  dice p in [0,1]  for each transaction; it decides the type
        final double p;
        // The rows are chosen once, so a retry after a deadlock repeats exactly the SAME transaction
        final int[] ids;
        final double[] start_range = new double[num_queries_per_task];
        final double[] stop_range = new double[num_queries_per_task];

        public Blocker(int id) {
            this.task = id;
            this.p = ThreadLocalRandom.current().nextDouble();
            this.ids = ThreadLocalRandom.current().ints(0, num_tuple).distinct().limit(num_queries_per_task).toArray();
        }

        // Every transaction must eventually commit: a deadlock victim is rolled back and retried
        @Override
        public void run() {
            long t0 = System.nanoTime();
            while (!attempt()) {
                deadlocks.incrementAndGet();
            }
            int k = p < 0.6 ? 0 : p < 0.7 ? 1 : p < 0.8 ? 2 : 3;
            typeNanos[k].add(System.nanoTime() - t0);
            typeCount[k].increment();
        }

        /** @return true when finished, false when aborted by a deadlock (then it is retried) */
        boolean attempt() {
            Connection con = null;
            try {
                con = openConnection();

                if (p < 0.6) {
                    //  read-only multi-point query, 3 distinct tuples, print the sum
                    double sum = 0.0;
                    try (PreparedStatement read = con.prepareStatement(
                            "SELECT data_value FROM blocking_data WHERE data_id = ?")) {
                        for (int rowId : ids) {
                            read.setInt(1, rowId);
                            try (ResultSet rs = read.executeQuery()) {
                                if (rs.next()) sum += rs.getDouble(1);
                            }
                        }
                    }
                    con.commit();
                    tasks_finished.incrementAndGet();
                    OUT.println("Task " + task + " read sum: " + sum);
                } else if (p < 0.7) {
                    //insert one new tuple (removed again by cerateDB() on the next start)
                    try (PreparedStatement insert = con.prepareStatement(
                            "INSERT INTO blocking_data (data_id, data_value, modified_by) VALUES (?, ?, ?)")) {
                        insert.setInt(1, num_tuple + task);
                        insert.setDouble(2, ThreadLocalRandom.current().nextDouble(max_value));
                        insert.setInt(3, task);
                        insert.executeUpdate();
                    }
                    con.commit();                                                          //does not wait for the disk
                    tasks_finished.incrementAndGet();
                    OUT.println("Task " + task + " inserted tuple " + (num_tuple + task));
                } else if (p < 0.8) {
                    // full table scan, print the sum
                    double sum;
                    try (PreparedStatement scan = con.prepareStatement("SELECT SUM(data_value) FROM blocking_data");
                         ResultSet rs = scan.executeQuery()) {
                        rs.next();
                        sum = rs.getDouble(1);
                    }
                    con.commit();
                    tasks_finished.incrementAndGet();
                    OUT.println("Task " + task + " table sum: " + sum);
                } else {
                    // write query, 3 updates, each modifies a single tuple
                    try (PreparedStatement update = con.prepareStatement(
                            "UPDATE blocking_data SET data_value = ?, modified_by = ? WHERE data_id = ?")) {
                        for (int rowId : ids) {
                            update.setDouble(1, ThreadLocalRandom.current().nextDouble(max_value));
                            update.setInt(2, task);
                            update.setInt(3, rowId);
                            update.executeUpdate();
                        }
                    }
                    con.commit();                                                          //does not wait for the disk
                    int n = tasks_finished.incrementAndGet();
                    OUT.println("Done task " + task + ". I am finisher number " + n);
                }
                return true;
            } catch (SQLException e) {
                try { if (con != null) con.rollback(); } catch (SQLException ignored) { }
                if ("40P01".equals(e.getSQLState())) return false;   // deadlock victim -> retry the same transaction
                System.err.println("Task " + task + " " + e.getMessage());
                return true;                                         // other error: do not loop forever
            } finally {
                try { if (con != null) con.close(); } catch (SQLException ignored) { }
            }
        }
    }
}