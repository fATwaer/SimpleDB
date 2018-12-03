package simpledb;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private OpIterator ch;
    
    
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        tid = t;
        ch = child;
    }

    public TupleDesc getTupleDesc() {
    	String[] name = { "DeleteSum" };
        Type[] type = { Type.INT_TYPE };
        return new TupleDesc(type, name);
    }
    
    private boolean open, fetched;
    Tuple next;
    public void open() throws DbException, TransactionAbortedException {
        ch.open();
        this.open = true;
        next = null;
        fetched = false;
    }

    public void close() {
        ch.close();
        this.open = false;
        next = null;
        fetched = false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	ch.rewind();
    	next = null;
    	fetched = false;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	int i = 0;
    	try {
        	while (ch.hasNext()) {
        		Database.getBufferPool().deleteTuple(tid, ch.next());
        		i++;
        	}
		} catch (NoSuchElementException e) {
			throw new DbException("no such element");
		} catch (IOException e) {
			throw new DbException("IO exception");
		}
        if (i == 0 && fetched)
        	return null;
        fetched = true;
        Tuple result  = new Tuple(getTupleDesc());
        result.setField(0, new IntField(i));
        return result;
    }

    
    public boolean hasNext() throws DbException, TransactionAbortedException {
    	if (!this.open)
    		throw new DbException("delete itr not open");
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
