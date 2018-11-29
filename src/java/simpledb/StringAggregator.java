package simpledb;

import java.util.ArrayList;
import java.util.NoSuchElementException;

import simpledb.Aggregator.Op;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    
    private Type gbtype;
    private int gbfield, afield;
    private Op op;
    private ArrayList<Tuple> list;
    private boolean noGrouping;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if (what != Op.COUNT)
        	throw new IllegalArgumentException();
        this.afield = afield;
        this.gbfield = gbfield;
        this.op = Op.COUNT;
        list = new ArrayList<Tuple>();
        this.noGrouping = (gbfield == Aggregator.NO_GROUPING ? true : false);
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    String[] name = null;
    Type[] types = null;
    public void mergeTupleIntoGroup(Tuple tup) {
   
    	for (Tuple t: list) {
    		if (!t.getField(gbfield).equals(tup.getField(gbfield)))
     		   continue;
    		int oldval = (((IntField)t.getField(1)).getValue());
    		t.setField(1, new IntField(oldval+1));
    		return;
    	}
    	// not implement `NO GROUPING`
    	if (name == null || types == null) {
     	   if (noGrouping) {
     		  name = new String[] {tup.getTupleDesc().getFieldName(afield)};
   	     	   types = new Type[] {Type.INT_TYPE};
     	   } else {
     		   name = new String[] {
				   tup.getTupleDesc().getFieldName(gbfield),
		 		   tup.getTupleDesc().getFieldName(afield)
		 	   };
	     	   types = new Type[] {
	     	       tup.getTupleDesc().getFieldType(gbfield),
	     	       Type.INT_TYPE
	     	   };
     	   }
        }
        Tuple t = new Tuple(new TupleDesc(types, name));
     	t.setField(1, new IntField(1));
     	if (!noGrouping)
     	   t.setField(0, tup.getField(gbfield));
        t.setField(noGrouping ? 0:1,new IntField(1));
        list.add(t);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
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
			if (this.open == false)
				throw new DbException("not open");
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
			index = 0;
		}
    }
}
