package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    
    private Predicate p;
    private OpIterator ch;
    private ArrayList<Tuple> tpp; // tuples pass the predication
    private ArrayList<OpIterator> itrs;
    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        this.p = p;
        ch = child;
    }

    public Predicate getPredicate() {
        return p;
    }

    public TupleDesc getTupleDesc() {
        return ch.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
    	ch.open();
    	this.open = true;
    }

    public void close() {
        ch.close();
        open = false;
        next = null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        ch.rewind();
    }
    private boolean open; 
    private Tuple next;
    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
    	Tuple t;
    	while (ch.hasNext()) {
    		t = ch.next();
    		if (p.filter(t))
    		{
    			next = t;
    			return next;
    		}
    	}
    	return null;
    }
    
    public boolean hasNext() throws DbException, TransactionAbortedException {
    	if (this.open != true)
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
    
    
    // I don't know whether the implementation of following methods is right   
    @Override
    public OpIterator[] getChildren() {
        return (OpIterator[]) itrs.toArray();
    }

    @Override
    public void setChildren(OpIterator[] children) {
        
        itrs = (ArrayList<OpIterator>) Arrays.asList(children); 
        
    }

}
