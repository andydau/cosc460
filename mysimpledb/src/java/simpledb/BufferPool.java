package simpledb;

import java.io.*;
import java.util.HashSet;
import java.util.Map;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.ArrayList;
import java.util.Set;

import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p/>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /**
     * Bytes per page, including header.
     */
    public static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;
    
    private LockManager lm;
    public LockManager getLockManager() { return lm; }

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;
    
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    
    private Map<PageId,Page> pages;
    private int maxSize;
    private LinkedList<PageId> order;
    
    public BufferPool(int numPages) {
    	this.pages = new Hashtable<PageId,Page>();
        this.maxSize = numPages;
        this.order = new LinkedList<PageId>();
        this.lm = new LockManager();
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p/>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm) throws 
    		DbException, TransactionAbortedException{
    	lm.acquireLock(pid, tid, perm);
    	if (this.order.contains(pid)){
			this.order.remove(pid);
		}
    	this.order.push(pid);
    	Page result = this.pages.get(pid);
   		if (result!=null){
   			return result;
   		}
   		if (this.pages.size()>=this.maxSize){
   			this.evictPage();	
    	}
    	int tableId = pid.getTableId();
   		DbFile table = Database.getCatalog().getDatabaseFile(tableId);
   		Page newPage = table.readPage(pid);
   		this.pages.put(pid, newPage);
   		return newPage;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        lm.releaseLock(tid, pid);                                                         // cosc460
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException,TransactionAbortedException {
    	transactionComplete(tid, true);
    }
    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2|lab3|lab4                                                         // cosc460
        return lm.hasLock(pid, tid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
            throws IOException {
    	if (commit){
    		//System.out.println("commit");
    		for (PageId pageId : this.pages.keySet()){
    			Page page = this.pages.get(pageId);
    			if (page.isDirty()==null){
    				continue;
    			}
    			if (page.isDirty().equals(tid)){
    				page.setBeforeImage();
    			}
    		}
    		//flushPages(tid);
    	}
    	else{
    		for (PageId pageId : this.pages.keySet()){
    			Page page = this.pages.get(pageId);
    			if (page.isDirty()==null){
    				continue;
    			}
    			if (page.isDirty().equals(tid)){
    				//page = page.getBeforeImage();
    				this.pages.put(pageId, page);
    			}
    		}
    	}
    	lm.removeFromGraph(tid);
    	lm.releaseLock(tid);
    	//Thread.currentThread().interrupt();
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed until lab5).                                  // cosc460
     * May block if the lock(s) cannot be acquired.
     * <p/>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have
     * been dirtied so that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
    	DbFile table = Database.getCatalog().getDatabaseFile(tableId);
    	ArrayList<Page> dirtied = table.insertTuple(tid, t);
    	for (int i = 0; i < dirtied.size();i++){
    		Page dirty = dirtied.get(i);
    		dirty.markDirty(true, tid);
    	}
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p/>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have
     * been dirtied so that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
    	Catalog cat = Database.getCatalog();
    	RecordId rid = t.getRecordId();
    	int tableId = rid.getPageId().getTableId();
    	if (!this.pages.containsKey(rid.getPageId())){
    		throw new DbException("can't find tuple");
    	}
//    	Iterator<Integer> it = cat.tableIdIterator();
//    	while (it.hasNext()){
//    		int tableId = it.next();
//    		DbFile table = cat.getDatabaseFile(tableId);
//    		try{
//    			Iterator<Page> dirty = table.deleteTuple(tid, t).iterator();
//    			while (dirty.hasNext()){
//    				Page dirtyPage = dirty.next();
//    				PageId pid = dirtyPage.getId();
//    				this.pages.put(pid, dirtyPage);
//    				dirtyPage.markDirty(true, tid);
//    			}
//    			return;
//    		}
//    		catch (DbException e){
//    			continue;
//    		}
//    	}
    	DbFile table = cat.getDatabaseFile(tableId);
		try{
			Iterator<Page> dirty = table.deleteTuple(tid, t).iterator();
			while (dirty.hasNext()){
				Page dirtyPage = dirty.next();
				PageId pid = dirtyPage.getId();
				this.pages.put(pid, dirtyPage);
				dirtyPage.markDirty(true, tid);
			}
			return;
		}
		catch (DbException e){
			e.printStackTrace();
		}
    	throw new DbException("can't find tuple");
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
    	PageId[] pids = (PageId[]) this.pages.keySet().toArray(new PageId[this.pages.size()]);
    	for (PageId pid : pids){
    		Page page = this.pages.get(pid);
    		if (page.isDirty()!=null){
    			flushPage(pid);
    		}
    	}
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     */
    public synchronized void discardPage(PageId pid) {
    	try{
    		this.pages.remove(pid);
    	}
    	catch (Exception e){
    		e.printStackTrace();
    	}
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
    	DbFile table = Database.getCatalog().getDatabaseFile(pid.getTableId());
    	Page page = this.pages.get(pid);
    	TransactionId dirtier = page.isDirty();
        if (dirtier != null){
          Database.getLogFile().logWrite(dirtier, page.getBeforeImage(), page);
          Database.getLogFile().force();
        }
    	table.writePage(page);
    	//System.out.println("Flushed "+pid);
    	this.pages.remove(page.getId());
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
    	Set<PageId> temp = new HashSet<PageId>(this.pages.keySet());
        for (PageId pageId : temp){
        	Page page = this.pages.get(pageId);
        	if (page.isDirty()==null){
        		continue;
        	}
        	if (page.isDirty().equals(tid)){
        		flushPage(pageId);
        		page.markDirty(false, tid);
        	}
        }
    }
    
    public synchronized void removePage(PageId pid){
    	if (this.pages.containsKey(pid)){
    		this.pages.remove(pid);
    	}
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException{
    	for (int i = this.order.size()-1; i>=0;i--)
    	{
    		PageId evictId = this.order.get(i);
    		try{
    			Page page = this.pages.get(evictId);
    			if (page==null){
    				throw new DbException("all pages dirty");
    			}
    			//if (page.isDirty()==null){
    				this.flushPage(evictId);
    				this.pages.remove(evictId);
    				this.order.remove(i);
    				return;
    			//}
    		}
    		catch (IOException e){
    			e.printStackTrace();
    			continue;
    		}
    	}
    	throw new DbException("All pages are dirty");
    }

}
