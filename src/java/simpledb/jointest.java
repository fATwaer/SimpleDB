package simpledb;

import java.io.File;

public class jointest {

	public static void main(String[] args) {
		Type[] types = new Type[] { Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE };
		String[] names = new String[] { "f0", "f1", "f2"};
		
		TupleDesc td = new TupleDesc(types, names);
		
		// two tables 
		HeapFile table1 = new HeapFile(new File("table1Heapfile.dat"), td);
		Database.getCatalog().addTable(table1, "t1");
		
		HeapFile table2 = new HeapFile(new File("table2Heapfile.dat"), td);
		Database.getCatalog().addTable(table2, "t2");
		
		// construct a query
		TransactionId tid = new TransactionId();
        SeqScan ss1 = new SeqScan(tid, table1.getId(), "t1");
        SeqScan ss2 = new SeqScan(tid, table2.getId(), "t2");
	
        // clause "where f0 > 1"
        Filter sf1 = new Filter (new Predicate(0, Predicate.Op.GREATER_THAN, new IntField(1)), ss1);
        
        JoinPredicate p = new JoinPredicate(1, Predicate.Op.EQUALS, 1);
        Join j = new Join(p, sf1, ss2);
        
        try {
        	j.open();
        	while (j.hasNext()) {
        		Tuple t = j.next();
        		System.out.println(t);
        	} 
        	j.close();
        	Database.getBufferPool().transactionComplete(tid);
        } catch (Exception e) {
        	e.printStackTrace();
        }
	}

}
