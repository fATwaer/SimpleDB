package simpledb;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    TransactionId tid;
    OpIterator ch;
    int tableId;
    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        if (!child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableId)))
        	throw new DbException("different desc");
        this.tid = t;
        this.ch = child;
        this.tableId = tableId;
    }

    public TupleDesc getTupleDesc() {
    	String[] name = { "InsertSum" };
        Type[] type = { Type.INT_TYPE };
        return new TupleDesc(type, name);
    }

    public void open() throws DbException, TransactionAbortedException {
        ch.open();
        this.open = true;
        next = null;
        this.fetched = false;
    }

    public void close() {
        ch.close();
        this.open = false;
        next = null;
        this.fetched = false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	ch.rewind();
    	next = null;
    	this.fetched = false;
    }
    private boolean open, fetched;
    Tuple next;
    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	
        int i = 0;
    	try {
        	while (ch.hasNext()) {
        		Database.getBufferPool().insertTuple(tid, tableId, ch.next());
        		i++;
        	}
		} catch (NoSuchElementException e) {
			throw new DbException("no such element");
		} catch (IOException e) {
			throw new DbException("IO exception");
		}
        if (i ==  0 && fetched)
        	return null;
        fetched = true;
        Tuple result  = new Tuple(getTupleDesc());
        result.setField(0, new IntField(i));
       return result;
    }

    public boolean hasNext() throws DbException, TransactionAbortedException {
    	if (!this.open)
    		throw new DbException("insert itr not open");
    	if (next == null)
    		next = fetchNext();
    		
    	return next != null;
    }
    
    public Tuple next() throws TransactionAbortedException, DbException {
    	if (!hasNext())
    		return null;
    	if (next == null)
    	{
    		next = fetchNext();
    		if (next == null)
    			throw new DbException("next is null");
    	}
    	Tuple r = next;
    	next = null;
    	return r;
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
