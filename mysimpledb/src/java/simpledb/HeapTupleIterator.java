package simpledb;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapTupleIterator implements DbFileIterator {
	
	private HeapFile file;
	private TransactionId tid;
	private int currentPage;
	private int maxPage;
	private Iterator<Tuple> currentIt;
	private BufferPool bp;
	private boolean open = false;
	
	public HeapTupleIterator(HeapFile file,TransactionId tid){
		this.file = file;
		this.tid = tid;
		this.bp = Database.getBufferPool();
		this.maxPage = file.numPages();
	};
	
	public void openPage(int pageNo) throws DbException, TransactionAbortedException{
		open = true;
		int tableId = file.getId();
		PageId pid = new HeapPageId(tableId, pageNo);
		HeapPage page = (HeapPage) bp.getPage(tid, pid, Permissions.READ_ONLY);
		currentIt = page.iterator();
	}
	
	public void open() throws DbException, TransactionAbortedException{
		open = true;
		currentPage = 0;
		openPage(currentPage);
	}
	public boolean hasNext(){
		if (!open){
			return false;
		}
		if (currentIt.hasNext()){
			return true;
		}
		if (currentPage<maxPage-1){
			currentPage++;
		}
		else{
			return false;
		}
		try{
			openPage(currentPage);
			return currentIt.hasNext();
		}
		catch (Exception e){
			return false;
		}
		
	}
	public Tuple next() throws NoSuchElementException{
		if (hasNext()){
			return currentIt.next();
		}
		else{
			throw new NoSuchElementException();
		}
	}
	public void close(){
		open = false;
	}
	public void rewind() throws DbException,TransactionAbortedException{
		open();
	}
}
