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
    	diskfile = f;
    	this.td = td;
    	newpage = 0;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
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
    	
    	return diskfile.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
    	int pgsz = BufferPool.getPageSize(); 
    	byte[] bytes = new byte[pgsz];
    	RandomAccessFile raf = null;
    	
    	int pgNo = pid.getPageNumber();
    	
    	int off = pgNo * pgsz;
    	HeapPage heapPage = null;
    	
    	// read a specific page from disk
    	try{
    		raf = new RandomAccessFile(diskfile, "r");
    		raf.seek(off);
			raf.read(bytes , 0, pgsz);
			heapPage =  new HeapPage(new HeapPageId(pid.getTableId(), pgNo), bytes);
			raf.close();
    	} catch (FileNotFoundException e) {
			throw new NoSuchElementException();
		} catch (IOException e) {
			throw new NoSuchElementException();
		}
    	
    	
        return heapPage;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    int newpage;
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
        return newpage + (int)pagenum;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
    		for (int i = 0; i <  numPages(); i++) {
    			HeapPage pg = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i), Permissions.READ_ONLY);
    			if (pg.getNumEmptySlots() == 0)
    				continue;
    			
    			pg.insertTuple(t);
    			
    			return new ArrayList<Page>() {{add(pg);}};
    		}
    		
    		
    		// no enough space, need allocate a new page
    		HeapPage pg =  (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), numPages()), Permissions.READ_ONLY);
    		pg.insertTuple(t);
    		// the page still reside on memory ,the number of pages will
    		// increment when bufferpool flush the memory ?
    		newpage++;
    		
    		return new ArrayList<Page>() {{add(pg);}};
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        for (int i = 0; i < numPages(); i++) {
        	HeapPage pg = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i), Permissions.READ_ONLY);
        	Iterator<Tuple> itr = pg.iterator();
        	while (itr.hasNext())
        		if (itr.next().equals(t))
        		{
        			pg.deleteTuple(t);

        			return new ArrayList<Page>() {{add(pg);}};
        		}
        }
        
        throw new DbException("tuple t is not a member of this DbFile");
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
    	
    	
        return new Itr(tid);
    }
    
    private class Itr implements DbFileIterator {
    	
    	TransactionId tid;
    	HeapPage curPage;
    	int pgNo, npages;
    	Iterator<Tuple> itr;
    	
    	Itr(TransactionId id) {
    		this.tid = id;
    		this.pgNo = 0;
    		this.curPage = null;
    		this.itr = null;
    		npages = numPages();
    	}
    	
		@Override
		public void open() throws DbException, TransactionAbortedException {
			curPage = (HeapPage) Database.getBufferPool().getPage(tid, 
					new HeapPageId(getId(), 0), Permissions.READ_ONLY);
			itr = curPage.iterator();
			pgNo = curPage.getId().getPageNumber();
			
			
			
			/** 
			 * use getPage() to load a page and 
			 * caching the page in the buffer pool
			 **/
//			try {
//				raf = new RandomAccessFile(diskfile, "r");
//				raf.read(data, 0, pgsz);
//				curPage = new HeapPage(new HeapPageId(getId(), pgNo), data);
//				
//				if (curPage == null)
//					return;
//				itr = curPage.iterator();
//				
//			} catch (FileNotFoundException e) {
//				throw new DbException("file not found");
//			} catch (IOException e) {
//				throw new DbException("iterator read page from disk error");
//			}
		}

		@Override
		public boolean hasNext() throws DbException, TransactionAbortedException {
			if ((pgNo + 1) > npages || itr == null)
				return false;

			if (itr.hasNext())
				return true;
			if ((pgNo + 1) == npages)
				return false;
			/** current page has been iterated absolutely     */
			/** just read the next page (here pgNo < npages) */
			pgNo++;
			curPage = (HeapPage) Database.getBufferPool().getPage(tid, 
					new HeapPageId(getId(), pgNo), Permissions.READ_ONLY);
			itr = curPage.iterator();
			
			return itr.hasNext();
		}

		@Override
		public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
			if (itr == null)
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
			/** release resource */
			itr = null;
			curPage = null;
		}
    	
    }
}

