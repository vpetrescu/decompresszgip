import java.io.PrintStream;
import java.util.*;

public class SnapshotIsolation {
	private static int max_xact = 0;
	
	 private static PrintStream log = System.out;
	 static HashMap<Integer, ArrayList<Integer>> okeys_to_tid;
//	 static HashMap<Integer, ArrayList<Transaction>> transactions;
	 static ArrayList<Transaction> transactions = new ArrayList<Transaction>();
	 
	 public class Transaction {
		 public class PairTimeValue {
			 public PairTimeValue(int t, int v) {
				 time_step = t;
				 value = v;
			 }
			 public int time_step;
			 public int value;
		 }
		 public Transaction(int t) {
			 tid = t;
			 begin_time = t;
			 commit_time = -1;
			 okeys_to_twrite = new HashMap<Integer, ArrayList<PairTimeValue>>();
			 okeys_to_tread = new HashMap<Integer, ArrayList<PairTimeValue>>();
		 }

		 int tid;
		 int begin_time;
		 int commit_time;
		 // maps object key to time id of write
		 public HashMap<Integer, ArrayList<PairTimeValue>> okeys_to_twrite;
		 // maps object key to time id of read
	 	 public HashMap<Integer, ArrayList<PairTimeValue>> okeys_to_tread;
	 	 // get last time of read
	 	 // has commited
	 }

	 static HashMap<Integer, Boolean> valid_transactions = new HashMap<Integer, Boolean>();
	 static HashMap<Integer,Integer> begin_transactions = new HashMap<Integer, Integer>();
	 static HashMap<Integer, Integer> commit_transactions = new HashMap<Integer, Integer>();
	 static HashMap<Integer, Set<Integer>> write_set = new HashMap<Integer, Set<Integer>>();
	 static ArrayList<TransactionEntry> entries = new ArrayList<TransactionEntry>();
	 public class TransactionEntry {
		 public TransactionEntry(char t, int k, int v, int tid) {
			 tr_type = t;
			 object_key = k;
			 object_value = v;
			 transaction_id = tid;
		 }
		 public char tr_type; // W, R, C, A
		 public int object_key;
		 public int object_value; // value that is read or written
		 public int transaction_id;
	 }
	/**
	 * @return transaction id == logical start timestamp
	 */
	public static int begin_transaction() {
		SnapshotIsolation a = new SnapshotIsolation();
		// transaction max_xact begins
		max_xact = max_xact +1;
		entries.add(a.new TransactionEntry('B', -1, -1, max_xact));
		begin_transactions.put(max_xact, entries.size());
		valid_transactions.put(max_xact, true);
		return max_xact;
	}

	/**
	 * @return value of object key in transaction xact
	 */
	public static int read(int xact, int key) throws Exception {
		// If this transaction has written to the object key before,
		// read that value
		// Else find the last value written by a transaction that commited
		// its changes
		// Else the value was not initialized
		boolean has_written_before = false;
		int value = 0;
		for (int i = entries.size()-1; i>=0; i--) {
			if (entries.get(i).transaction_id == xact && 
					valid_transactions.get(xact) == true && // consider avoiding this
					entries.get(i).tr_type == 'W' &&
					entries.get(i).object_key == key) {
				value = entries.get(i).object_value;
				SnapshotIsolation a = new SnapshotIsolation();
				entries.add(a.new TransactionEntry('R', key, value, xact));
				has_written_before = true;
				break;
			}
		}
		if (has_written_before == false) {
			for (int i = entries.size()-1; i>=0; i--) {
				int tid_other = entries.get(i).transaction_id;
				if ( tid_other != xact && commit_transactions.containsKey(tid_other) &&
						begin_transactions.get(xact) > commit_transactions.get(tid_other) && // consider avoiding this
						entries.get(i).tr_type == 'W' &&
						entries.get(i).object_key == key) {
					value = entries.get(i).object_value;
					SnapshotIsolation a = new SnapshotIsolation();
					entries.add(a.new TransactionEntry('R', key, value, xact));
					has_written_before = true;
					break;
				}
			}
		}
		if (has_written_before == false) {
			// Reading uniitialized value...
			SnapshotIsolation a = new SnapshotIsolation();
			entries.add(a.new TransactionEntry('R', key, value, xact));
		}
		
		log.println(String.format("T(%d):R(%d) => %d", xact, key, value));
		return value;
	}

	/**
	 * write value of existing object identified by key in transaction xact
	 */
	public static void write(int xact, int key, int value) throws Exception {
		SnapshotIsolation a = new SnapshotIsolation();
		entries.add(a.new TransactionEntry('W', key, value, xact));
		// add object to write set of xact
		if (write_set.containsKey(xact)) {
			Set<Integer> s = write_set.get(xact);
			s.add(key);
			write_set.put(xact, s);
		} else {
			Set<Integer> s = new HashSet<Integer>();
			s.add(key);
			write_set.put(xact, s);
		}
		
		log.println(String.format("T(%d):W(%d,%d)", xact, key, value));
	}

	public static void commit(int xact) throws Exception {
		if (valid_transactions.get(xact) == false) {
			rollback(xact);
			return;
		}
		 // Check Write- Write issues
		SnapshotIsolation a = new SnapshotIsolation();
		entries.add(a.new TransactionEntry('C', -1, -1, xact));
		commit_transactions.put(xact, entries.size());
		// loop to see if there are transactions that began written while I was in the middle of
		// writing.
		int btime = begin_transactions.get(xact);
		int ctime = commit_transactions.get(xact);
		for (Map.Entry<Integer, Integer> e: begin_transactions.entrySet()) {
			// if transaction started in the middle of this transaction,
			// and their write set is not null, abort that transaction
			if (e.getKey() != xact && e.getValue() > btime &&
					e.getValue() < ctime) {
				// find intersection set
				Set<Integer>s1 = write_set.get(xact);
				Set<Integer>s2 = write_set.get(e.getKey());
				s1.retainAll(s2);
				if (s1.size() != 0) {
					// abort the other transaction somehow
					valid_transactions.put(e.getKey(), false);
				}
			}
			// if transaction started in the middle of this transaction,
			// and their write set is not null, abort that transaction
			if (e.getKey() != xact && e.getValue() < btime &&
					commit_transactions.containsKey(e.getKey()) == false) {
				// find intersection set
				if (write_set.containsKey(xact) && write_set.containsKey(e.getKey())) {
					Set<Integer>s1 = write_set.get(xact);
					Set<Integer>s2 = write_set.get(e.getKey());
					if (s1.size() == 0) {
						valid_transactions.put(e.getKey(), false);
					} else {
						s1.retainAll(s2);
						if (s1.size() != 0) {
							// abort the other transaction somehow
							valid_transactions.put(e.getKey(), false);
						}
					}
				}
			}
		}
		
		 log.println(String.format("T(%d):COMMIT SUCCESSFUL", xact));
	}

	public static void rollback(int xact) throws Exception {
		 log.println(String.format("T(%d):ROLLBACK", xact));
	}
}
