package simpledb;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see simpledb.HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
	
	private File file;
	private TupleDesc tuple;
	private int id;
	
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.tuple = td;
        this.id = f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.id;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
       return this.tuple;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
    	int pageNo = pid.pageNumber();
    	int offSet = pageNo * BufferPool.PAGE_SIZE;
    	try{
    		InputStream reader = new FileInputStream(this.file);
    		byte[] buffer = new byte[BufferPool.PAGE_SIZE];
    		reader.skip(offSet);
    		reader.read(buffer);
    		reader.close();
    		HeapPageId hid = (HeapPageId) pid;
    		return new HeapPage(hid,buffer);
    	}
    	catch (IOException e){
    		e.printStackTrace();
    		return null;
    	}
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
    	PageId pid = page.getId();
    	int pageNo = pid.pageNumber();
    	int offSet = pageNo * BufferPool.PAGE_SIZE;
    	RandomAccessFile file = new RandomAccessFile(this.file,"rw");
    	file.skipBytes(offSet);
    	file.write(page.getPageData());
    	file.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.ceil((float)file.length()/BufferPool.PAGE_SIZE);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        BufferPool bp = Database.getBufferPool();
        ArrayList<Page> result = new ArrayList<Page>();
        for (int i = 0; i < this.numPages();i++){
        	HeapPageId pid = new HeapPageId(this.getId(),i);
        	HeapPage hp = (HeapPage) bp.getPage(tid, pid, Permissions.READ_ONLY);
        	if (hp.getNumEmptySlots()>0){
        		hp = (HeapPage) bp.getPage(tid, pid, Permissions.READ_WRITE);
        		hp.insertTuple(t);
        		//hp.markDirty(true, tid);
        		result.add(hp);
        		return result;
        	}
        }
        synchronized (this){
        	HeapPageId newId = new HeapPageId(this.getId(),this.numPages());
        	byte[] data = HeapPage.createEmptyPageData();
        	HeapPage newPage = new HeapPage(newId,data);
        	newPage.insertTuple(t);
        	//newPage.markDirty(true, tid);
        	OutputStream writer = new BufferedOutputStream(new FileOutputStream(this.file,true));
        	writer.write(newPage.getPageData());
        	writer.close();
        	newPage = (HeapPage) bp.getPage(tid, newId, Permissions.READ_ONLY);
        	result.add(newPage);
        	return result;
        }
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        PageId pid = t.getRecordId().getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        page.deleteTuple(t);
        //page.markDirty(true, tid);
        ArrayList<Page> result = new ArrayList<Page>();
        result.add(page);
        return result;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapTupleIterator(this,tid);
    }

}

