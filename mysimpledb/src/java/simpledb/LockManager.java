package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
	private HashMap<PageId,TransactionId> xmap;
	private HashMap<PageId,ArrayList<TransactionId>> smap;

	public LockManager(){
		xmap = new HashMap<PageId,TransactionId>();
		smap = new HashMap<PageId,ArrayList<TransactionId>>();
	}
    public synchronized void acquireLock(PageId pid, TransactionId tid, Permissions perm) {
        boolean waiting = true;
        while (waiting) {
            if (perm.equals(Permissions.READ_WRITE)){
            	if (!xmap.containsKey(pid)) {
                	// it's not in use, so we can take it!
            		if (!smap.containsKey(pid)){
            			xmap.put(pid,tid);
            			waiting = false;
            		}
            		else{
            			ArrayList<TransactionId> tids = smap.get(pid);
            			for (TransactionId id : tids){
            				if (id.equals(tid)){
            					if (this.upgradable(pid, tid)){
            						this.upgrade(pid, tid);
            						waiting = false;
            						break;
            					}
            				}
            			}
            		}
            	}
            	else{
            		if (xmap.get(pid).equals(tid)){
            			waiting = false;
            		}
            	}
            }
            if (perm.equals(Permissions.READ_ONLY)){
            	if (!xmap.containsKey(pid)) {
                	// it's not in use, so we can take it!
            		if (!smap.containsKey(pid)){
            			ArrayList<TransactionId> newList = new ArrayList<TransactionId>();
            			newList.add(tid);
            			smap.put(pid,newList);
            		}
            		else{
            			ArrayList<TransactionId> list = smap.get(pid);
            			list.add(tid);
            			smap.put(pid, list);
            		}
            		waiting = false;
            	}
            	else{
            		if (xmap.get(pid).equals(tid)){
            			waiting = false;
            		}
            	}
            }
            if (waiting) {
                try {
                	wait();
                } catch (InterruptedException ignored) {
                	break;
                }
            }
        }
    }

    public synchronized void releaseLock(TransactionId tid, PageId pid) {
    	if (xmap.containsKey(pid)){
    		TransactionId id = xmap.get(pid);
    		if (id.equals(tid)){
    			xmap.remove(pid);
    		}
    	}
    	if (smap.containsKey(pid)){
    		smap.get(pid).remove(tid);
    		if (smap.get(pid).size()==0){
    			smap.remove(pid);
    		}
    	}
        notifyAll();
    }
    
    public synchronized boolean hasLock(PageId pid, TransactionId tid){
    	if (xmap.containsKey(pid)){
    		if (xmap.get(pid).equals(tid)){
    			return true;
    		}
    	}
    	if (smap.containsKey(pid)){
    		for (TransactionId id : smap.get(pid)){
    			if (id.equals(tid)){
    				return true;
    			}
    		}
    	}
    	return false;
    }
    
    public synchronized boolean upgradable(PageId pid, TransactionId tid){
    	if (!xmap.containsKey(pid)){
    		return true;
    	}
    	return false;
    }
    
    public synchronized void upgrade(PageId pid, TransactionId tid){
    	xmap.put(pid, tid);
    	smap.get(pid).remove(tid);
    	smap.get(pid).add(0, tid);
    }
}
