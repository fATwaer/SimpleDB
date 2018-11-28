package simpledb;

import java.util.ArrayList;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    
    private int gbfield;
    private Type type;
    private int afield;
    private Op op;
    private ArrayList<Tuple> list;
    
    // for calculate the average
    private ArrayList<Integer> avg; 
    
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
    	this.gbfield = gbfield;
    	this.type = gbfieldtype;
    	this.afield = afield;
    	this.op = what;
    	list = new ArrayList<Tuple>();
    	avg = new ArrayList<Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    String[] name = null;
    Type[] types = null;
    public void mergeTupleIntoGroup(Tuple tup) {
       for (Tuple t : list) {
    	   if (!t.getField(gbfield).equals(tup.getField(gbfield)))
    		   continue;
    	   
    	   int newval = ((IntField)tup.getField(afield)).getValue();
    	   int oldval = (((IntField)t.getField(afield)).getValue());
    	   
    	   switch (op) {
    	   		case AVG:
    	   			if (avg.size() == 0)
    	   				avg.add(oldval);
    	   			avg.add(newval);
    	   			int sum = 0;
    	   			for (int i : avg)
    	   				sum += i;
    	   			t.setField(1, new IntField(sum / avg.size()));
    	   			break;
    	   		case MAX:
    	   			t.setField(1, new IntField(newval > oldval ? newval: oldval));
    	   			break;
    	   		case MIN:
    	   			t.setField(1, new IntField(newval < oldval ? newval: oldval));
    	   			break;
    	   		case COUNT:
    	   			t.setField(1, new IntField(oldval+1));
    	   			break;
    	   		case SUM:
    	   			t.setField(1, new IntField(oldval+newval));
    	   			break;
    	   }
    	   return;
       }
       if (name == null || types == null) {
    	   name = new String[] {
    		   tup.getTupleDesc().getFieldName(gbfield),
    		   tup.getTupleDesc().getFieldName(afield)
    	   };
    	   types = new Type[] {
    	       tup.getTupleDesc().getFieldType(gbfield),
    	       tup.getTupleDesc().getFieldType(afield)
    	   };
       }
       Tuple t = new Tuple(new TupleDesc(types, name));
       t.setField(0, tup.getField(gbfield));
       if (op != Op.COUNT)
    	   t.setField(1, tup.getField(afield));
       else
    	   t.setField(1, new IntField(1));
       list.add(t);
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
		return new Itr();
    }
    
    public class Itr implements OpIterator {

    	private boolean open;
    	private int index;
    	
		@Override
		public void open() throws DbException, TransactionAbortedException {
			this.open = true;
			index = 0;
		}

		@Override
		public boolean hasNext() throws DbException, TransactionAbortedException {
			
			if (index >= list.size())
				return false;
			return true;
		}

		@Override
		public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
			if (!hasNext())
				throw new NoSuchElementException();
			return list.get(index++);
		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			index = 0;
			
		}

		@Override
		public TupleDesc getTupleDesc() {
			return list.get(0).getTupleDesc();
		}

		@Override
		public void close() {
			this.open = false;
		}
    	
    }

}
