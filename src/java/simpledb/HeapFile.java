package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
	
	private File diskfile;
	private TupleDesc td;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
    	diskfile = f;
    	this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return diskfile;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
    	return diskfile.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
    	int pgsz = BufferPool.getPageSize(); 
    	byte[] bytes = new byte[pgsz];
    	
    	int pgNo = pid.getPageNumber();
    	int off = pgNo * BufferPool.getPageSize();
    	HeapPage heapPage = null;
    	
    	try (RandomAccessFile raf = new RandomAccessFile(diskfile, "r")){
			 
			raf.read(bytes , off, pgsz);
			heapPage =  new HeapPage(new HeapPageId(pid.getTableId(), pgNo), bytes);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
    	
    	
        return heapPage;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
    	long filesize = diskfile.length();
    	
    	
    	long pagenum = filesize / BufferPool.getPageSize();
    	// Roundup
    	if (pagenum * BufferPool.getPageSize() < filesize)
    		pagenum++;
    	
    	/** !!! cast (long -> int) here*/
        return (int)pagenum;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
    	
    	
        return new Itr(tid);
    }
    
    private class Itr implements DbFileIterator {
    	
    	TransactionId tid;
    	RandomAccessFile raf;
    	HeapPage curPage;
    	int pgNo, npages, pgsz;
    	byte[] data;
    	Iterator<Tuple> itr;
    	
    	Itr(TransactionId id) {
    		this.tid = id;
    		this.pgNo = 0;
    		this.curPage = null;
    		this.pgsz = BufferPool.getPageSize();
    		this.data = new byte[pgsz];
    		this.itr = null;
    		npages = numPages();
    	}
    	
		@Override
		public void open() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			try {
				raf = new RandomAccessFile(diskfile, "r");
				raf.read(data, pgNo*pgsz, pgsz);
				curPage = new HeapPage(new HeapPageId(getId(), pgNo), data);
				
				if (curPage == null)
					return;
				itr = curPage.iterator();
				
			} catch (FileNotFoundException e) {
				throw new DbException("file not found");
			} catch (IOException e) {
				throw new DbException("iterator read page from disk error");
			}
		}

		@Override
		public boolean hasNext() throws DbException, TransactionAbortedException {
			
			if ((pgNo + 1) > npages)
				return false;
			if (itr == null)
				return false;
			if (itr.hasNext())
				return true;
			if ((pgNo + 1) == npages)
				return false;
			/** current page has been iterated absolutely     */
			/** just read the later page (here pgNo < npages) */
			pgNo++;
			try {
				raf.read(data, pgNo*pgsz, pgsz);
				curPage = new HeapPage(new HeapPageId(getId(), pgNo), data);
				itr = curPage.iterator();
			} catch (IOException e) {
				throw new DbException("iterator read page from disk error");
			}
			
			return itr.hasNext();
		}

		@Override
		public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
			if (raf == null || itr == null)
				throw new NoSuchElementException();
				
			return itr.next();
		}
		
		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			pgNo = 0;
			open();
			
		}

		@Override
		public void close() {
			try {
				raf.close();
				itr = null;
			} catch (IOException e) {
				throw new NoSuchElementException();
			}
		}
    	
    }
}

