package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    
    private OpIterator ch;
    private int af, gf;
    private Aggregator.Op op;
    private OpIterator aggItr;
    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use 
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
		ch = child;
		af = afield;
		gf = gfield;
		op = aop;
	
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
		if (gf == -1)
			return Aggregator.NO_GROUPING;
		return gf;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
    	if (gf == -1)
    		return null;
    	return aggItr.getTupleDesc().getFieldName(0);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
    	return af;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	    	return gf == -1 ? aggItr.getTupleDesc().getFieldName(0):
	    					aggItr.getTupleDesc().getFieldName(1);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
    	return op;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
    	return aop.toString();
    }

    private boolean open;
    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
    	this.open = true;
    	
    	// merge
		ch.open();
		if (ch.getTupleDesc().getFieldType(af) == Type.INT_TYPE) {
			IntegerAggregator iagg = new IntegerAggregator(gf, Type.INT_TYPE, af, op);
			while (ch.hasNext())
				iagg.mergeTupleIntoGroup(ch.next());
			aggItr = iagg.iterator();
		} else {
			StringAggregator sagg = new StringAggregator(gf, Type.STRING_TYPE, af, op);
			while (ch.hasNext())
				sagg.mergeTupleIntoGroup(ch.next());
			aggItr = sagg.iterator();
		}
		ch.close();
		
		
    	aggItr.open();
    	next = null;
    }
    Tuple next;
    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	if (next == null)
    		if (aggItr.hasNext())
    			next = aggItr.next();
    		else
    			return null;
    	return next;
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	aggItr.rewind();
    	next = null;
    }
    public boolean hasNext() throws DbException, TransactionAbortedException {
    	if (this.open == false)
    		throw new DbException("not open");
    	if (next == null)
    		fetchNext();
    	return next != null;
    }
    
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
    	if (next == null) {
    		fetchNext();
    		if (next == null)
    			throw new NoSuchElementException();
    	}
    	Tuple result = next;
    	next = null;
    	return result;
    }
    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
    	String[] name;
    	Type[] types;
    	if (gf == -1)
    	{

    		name = new String[] {"aggName("+(op.toString())+")"+ch.getTupleDesc().getFieldName(af)};
    		types = new Type[] {ch.getTupleDesc().getFieldType(af)};
    	} else {
    		name = new String[] {
        			ch.getTupleDesc().getFieldName(gf),
        			"aggName("+(op.toString())+")"+ch.getTupleDesc().getFieldName(af)
        		};
    		types = new Type[] {
    			ch.getTupleDesc().getFieldType(gf),
    			ch.getTupleDesc().getFieldType(af)
    		};
    	} 
    	return new TupleDesc(types, name); 
    	
    }

    public void close() {
    	aggItr.close();
    	next = null;
    	this.open = false;
    }

    @Override
    public OpIterator[] getChildren() {
	// some code goes here
	return null;
    }

    @Override
    public void setChildren(OpIterator[] children) {
	// some code goes here
    }
    
}
