/**
 *  This is an example test file. Try others to debug your SnapshotIsolation impl.
 *  
 * @author Christoph Koch (christoph.koch@epfl.ch)
 *
 */
public class SnapshotIsolationTest1 {
	// This is an example test file. Try others to debug your system!!!
	
	public static void main(String[] args) {
		try {
			/* Example schedule:
			 T1: I(1) C
			 T2:        R(1) W(1)           R(1) W(1) C
			 T3:                  R(1) W(1)             C
			*/
			int t1 = SnapshotIsolation.begin_transaction();
			int obj1 = 1;
			SnapshotIsolation.write(t1, obj1, 13); // create obj1 and initialize with 13
			SnapshotIsolation.commit(t1);
			int t2 = SnapshotIsolation.begin_transaction();
			int t3 = SnapshotIsolation.begin_transaction();
			SnapshotIsolation.write(t2, obj1, SnapshotIsolation.read(t2, obj1) * 2); // double value of obj1
			SnapshotIsolation.write(t3, obj1, SnapshotIsolation.read(t3, obj1) + 4); // increment value of obj1 by 4

			SnapshotIsolation.write(t2, obj1, SnapshotIsolation.read(t2, obj1) * 2); // double value of obj1

			SnapshotIsolation.commit(t2);
			SnapshotIsolation.commit(t3);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
