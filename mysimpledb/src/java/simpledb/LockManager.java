package simpledb;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
	private ConcurrentHashMap<PageId,TransactionId> xmap;
	private ConcurrentHashMap<PageId,ArrayList<TransactionId>> smap;

	public LockManager(){
		xmap = new ConcurrentHashMap<PageId,TransactionId>();
	}
    public synchronized void acquireLock(PageId pid, TransactionId tid) {
        boolean waiting = true;
        while (waiting) {
            // check if lock is available
            if (!xmap.containsKey(pid)) {
                // it's not in use, so we can take it!
                xmap.put(pid,tid);
                waiting = false;
            }
            if (xmap.get(pid).equals(tid)){
            	waiting = false;
            }
            if (waiting) {
                try {
                	wait();
                } catch (InterruptedException ignored) { }
            }
        }
    }

    public synchronized void releaseLock(PageId pid) {
        xmap.remove(pid);
        notifyAll();
    }
    
    public synchronized boolean hasLock(PageId pid, TransactionId tid){
    	if (!xmap.containsKey(pid)){
    		return false;
    	}
    	if (!xmap.get(pid).equals(tid)){
    		return false;
    	}
    	return true;
    }
}
