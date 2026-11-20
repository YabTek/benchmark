import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

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
	public static int num_taks=30;
	public static int num_queries_per_task = 3;
	
	Connection con;
	Random rand = new Random(123456);
	int num_tuple = 50000;
	double max_value = 50.0f;
	AtomicInteger tasks_finished;
	
	public Blocking(){
		tasks_finished = new AtomicInteger(0);  //AtomicInteger provides thread-safe operations, while updating the same int value
	}
	
	void executeSQL(String sql, Connection con){
		java.sql.Statement stmt;
		try {
			stmt = con.createStatement();
			stmt.execute(sql);
			System.out.println("Executing "+sql);
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	void cerateDB(){	
		try {
			Connection con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
			executeSQL("DROP TABLE IF EXISTS blocking_data;",con);
			executeSQL("CREATE TABLE blocking_data ( data_id integer primary key, data_value float, modified_by integer);",con);
			
			PreparedStatement ps;
			con.setAutoCommit(false); //Setting autoCommit to false allows multiple database operations to be grouped into one transaction.
			ps = con.prepareStatement("INSERT INTO blocking_data VALUES (?, ?, ?)",ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
			
			for(int i=0;i<num_tuple;i++){ // num_tuple
				ps.setInt(1, i);

				ps.setDouble(2,rand.nextDouble()*max_value);

				ps.setInt(3, 0);

				ps.addBatch();
				ps.clearParameters();
			}


			
			int[] results = ps.executeBatch();
			con.commit();
			con.close();
			System.out.println(Arrays.toString(results));
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
//	public static void main(String[] args){
//		//Create DB
//		Blocking b = new Blocking();
//		b.cerateDB();
//
//		//Create task and their queries
//		ArrayList<Blocking.Blocker> my_tasks = new ArrayList<Blocking.Blocker>(num_taks);
//
//		for(int task=0;task<num_taks;task++){
//			my_tasks.add(b.new Blocker(task)); //This creates 30 Blocker objects with IDs from 0 to 29.
//		}
//
//		//Execute tasks
//		ThreadPoolExecutor executor =
//				  (ThreadPoolExecutor) Executors.newFixedThreadPool(4);//XXX - Do not change the number of threads used
//		double start, stop;
//		start = System.currentTimeMillis();
//		for(Blocker task : my_tasks) {
//			executor.execute(task);
//		}
//		executor.shutdown();
//
//		//wait for the tasks to finish
//		while(!executor.isTerminated()) {
//			try {
//				Thread.sleep(100); //Keep waiting every 0.1s until the executor is completely done.
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
//		}
//		stop = System.currentTimeMillis();
//		System.out.println("[DONE] Task execution after roughly "+(stop-start)+" ms finished: "+b.tasks_finished+" of "+num_taks);
//	}

    //2. continuous stream
    public static void main(String[] args) throws InterruptedException {
        Blocking b = new Blocking();
        b.cerateDB();

        //int workers = 4;

        //3. vary number of threads
        int workers = args.length > 0 ? Integer.parseInt(args[0]) : 4;

        long durationNanos = java.util.concurrent.TimeUnit.SECONDS.toNanos(30);
        AtomicInteger nextTaskId = new AtomicInteger(0);

        var executor = Executors.newFixedThreadPool(workers);
        long start = System.nanoTime();

        for (int worker = 0; worker < workers; worker++) {
            executor.execute(() -> {
                while (System.nanoTime() - start < durationNanos) {
                    int taskId = nextTaskId.getAndIncrement();
                    b.new Blocker(taskId).run();
                }
            });
        }

        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, java.util.concurrent.TimeUnit.NANOSECONDS);


        //3. Add time measure for benchmarking the throughput
        double elapsedSeconds = (System.nanoTime() - start) / 1_000_000_000.0;
        int committed = b.tasks_finished.get();

        System.out.println("Worker threads: " + workers);
        System.out.println("Transactions started: " + nextTaskId.get());
        System.out.println("Transactions committed: " + committed);
        System.out.printf("Elapsed time: %.3f seconds%n", elapsedSeconds);
        System.out.printf("Throughput: %.2f commits/second%n",
                committed / elapsedSeconds);
    }
	
	/**
	 * This is simply a task attempting to hold many write locks for a long time and updating major parts of the database.
	 *
	 */
	public class Blocker implements Runnable  {
		final int task;
        //1a
        final double p;
		final double [] start_range;
		final double [] stop_range;
		
		public Blocker(int id){
			this.task = id;
            //1a
            this.p = java.util.concurrent.ThreadLocalRandom.current().nextDouble();
			this.start_range = new double[num_queries_per_task];
			this.stop_range  = new double[num_queries_per_task];
			for(int q=0;q<num_queries_per_task;q++) {
				stop_range[q]  = rand.nextDouble()*max_value;

				start_range[q] = rand.nextDouble()*stop_range[q]; // stop_range * (0-1)

			}
		}
		
		@Override
		public void run() {
			Connection con=null;//XXX - Yes there needs to be a separate Connection per blocking taks. Otherwise strange things can happen
			con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);//XXX This is the class you need to create, but does not have to be in the Repo

			try {
				con.setAutoCommit(false);
				con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED); //It prevents a transaction from reading uncommitted changes made by another transaction.

                //1b
                if (p < 0.6) {
                    var random = java.util.concurrent.ThreadLocalRandom.current();

                    int first = random.nextInt(num_tuple);

                    int second;
                    do {
                        second = random.nextInt(num_tuple);
                    } while (second == first);

                    int third;
                    do {
                        third = random.nextInt(num_tuple);
                    } while (third == first || third == second);

                    double sum = 0.0;
                    String sql = "SELECT data_value FROM blocking_data WHERE data_id = ?";

                    try (PreparedStatement read = con.prepareStatement(sql)) {
                        for (int rowId : new int[] {first, second, third}) {
                            read.setInt(1, rowId);

                            try (ResultSet result = read.executeQuery()) {
                                if (!result.next()) {
                                    throw new SQLException("Row " + rowId + " was not found");
                                }
                                sum += result.getDouble("data_value");
                            }
                        }
                    }

                    con.commit();
                    tasks_finished.incrementAndGet();
                    con.close();

                    System.out.println("Task " + task + " read sum: " + sum);
                    return;
                }

                //1c
                if (p >= 0.6 && p < 0.7) {
                    int newId = num_tuple + task;
                    double newValue = java.util.concurrent.ThreadLocalRandom.current()
                            .nextDouble(max_value);

                    String sql = "INSERT INTO blocking_data (data_id, data_value, modified_by) "
                            + "VALUES (?, ?, ?)";

                    try (PreparedStatement insert = con.prepareStatement(sql)) {
                        insert.setInt(1, newId);
                        insert.setDouble(2, newValue);
                        insert.setInt(3, task);
                        insert.executeUpdate();
                    }

                    con.commit();
                    tasks_finished.incrementAndGet();
                    con.close();
                    return;
                }

                //1d
                if (p >= 0.7 && p < 0.8) {
                    String sql = "SELECT SUM(data_value) FROM blocking_data";
                    double sum;

                    try (PreparedStatement scan = con.prepareStatement(sql);
                         ResultSet result = scan.executeQuery()) {
                        result.next();
                        sum = result.getDouble(1);
                    }

                    con.commit();
                    tasks_finished.incrementAndGet();
                    con.close();

                    System.out.println("Task " + task + " table sum: " + sum);
                    return;
                }

                //1e
                var random = java.util.concurrent.ThreadLocalRandom.current();
                String sql = "UPDATE blocking_data "
                        + "SET data_value = ?, modified_by = ? "
                        + "WHERE data_id = ?";

                try (PreparedStatement update = con.prepareStatement(sql)) {
                    for (int q = 0; q < num_queries_per_task; q++) {
                        int rowId = random.nextInt(num_tuple);
                        double newValue = random.nextDouble(max_value);

                        update.setDouble(1, newValue);
                        update.setInt(2, task);
                        update.setInt(3, rowId);
                        update.executeUpdate();
                    }
                }

				con.commit();
				int num_finished = tasks_finished.incrementAndGet();
				con.close();
				System.out.println("Done task "+task+". I am finisher number "+num_finished);
			} catch (SQLException e) {
				System.err.println("Task "+task+" "+e.getMessage());
			}
		}
	}
}
