package simpledb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
	private HashMap<PageId,TransactionId> xmap;
	private HashMap<PageId,HashSet<TransactionId>> smap;
	private WaitGraph wgraph;

	public LockManager(){
		xmap = new HashMap<PageId,TransactionId>();
		smap = new HashMap<PageId,HashSet<TransactionId>>();
		wgraph = new WaitGraph();
	}
    public synchronized void acquireLock(PageId pid, TransactionId tid, Permissions perm) 
    		throws TransactionAbortedException {
    	
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
            			HashSet<TransactionId> tids = smap.get(pid);
            			if (tids.size()==1){
            				if (tids.contains(tid)){
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
            			HashSet<TransactionId> newList = new HashSet<TransactionId>();
            			newList.add(tid);
            			smap.put(pid,newList);
            		}
            		else{
            			HashSet<TransactionId> list = smap.get(pid);
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
                	//wgraph.printEdges();
                	ArrayList<TransactionId> waits = new ArrayList<TransactionId>();
                	if (xmap.containsKey(pid)){
                		waits.add(xmap.get(pid));
                	}
                	if (smap.containsKey(pid)){
                		waits.addAll(smap.get(pid));
                	}
                	for (int i = 0; i < waits.size();i++){
                		TransactionId current = waits.get(i);
                		if (wgraph.checkCycle(tid, current)){
                			BufferPool bp = Database.getBufferPool();
            				bp.transactionComplete(tid, false);
            				throw new TransactionAbortedException();
                		}
                	}
                	for (int i = 0; i < waits.size();i++){
                		TransactionId current = waits.get(i);
                		wgraph.addEdge(tid, current);
                	}
                	wait();
                } catch (InterruptedException ignored) {
                	break;
                } catch (IOException e){
                	e.printStackTrace();
                }
            }
        }
    }

    public synchronized void releaseLock(TransactionId tid) {
    	HashMap<PageId,TransactionId> temp = (HashMap<PageId,TransactionId>) xmap.clone();
    	for (PageId pid : temp.keySet()){
    		TransactionId id = xmap.get(pid);
    		if (id.equals(tid)){
    			xmap.remove(pid);
    		}
    	}
    	if (xmap==null){
    		xmap = new HashMap<PageId,TransactionId>();
    	}
    	HashMap<PageId,ArrayList<TransactionId>> temp2 = (HashMap<PageId,ArrayList<TransactionId>>) smap.clone();
    	for (PageId pid : temp2.keySet()){
    		smap.get(pid).remove(tid);
    		if (smap.get(pid).size()==0){
    			smap.remove(pid);
    		}
    	}
    	if (smap==null){
    		smap = new HashMap<PageId,HashSet<TransactionId>>();
    	}
        notifyAll();
    }
    public synchronized void releaseLock(TransactionId tid, PageId pid){
    	if (xmap.containsKey(pid)){
    		if (xmap.get(pid).equals(tid)){
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
    	smap.remove(pid);
    }

    public synchronized void removeFromGraph(TransactionId tid){
    	wgraph.remove(tid);
    }
private class WaitGraph{
	private ConcurrentHashMap<TransactionId,HashSet<TransactionId>> edges;
	
	public WaitGraph(){
		edges = new ConcurrentHashMap<TransactionId,HashSet<TransactionId>>();
	}
	
	public boolean hasEdge(TransactionId t1,TransactionId t2){
		if (edges.contains(t1)){
			if (edges.get(t1).contains(t2)){
				return true;
			}
		}
		return false;
	}
	
	public void addEdge(TransactionId t1, TransactionId t2){
		if (edges.containsKey(t1)){
			edges.get(t1).add(t2);
		}
		else {
			HashSet<TransactionId> neighbors = new HashSet<TransactionId>();
			neighbors.add(t2);
			edges.put(t1, neighbors);
		}
	}
	
	public void printEdges(){
		System.out.println("Print Graph:");
		for (TransactionId tid : edges.keySet()){
			HashSet<TransactionId> neighbors = edges.get(tid);
			System.out.print(tid+" ");
			for (TransactionId t1 : neighbors){
				System.out.print(t1+" ");
			}
			System.out.println();
		}
	}
	
	public void remove(TransactionId tid){
		if (edges.containsKey(tid)){
			edges.remove(tid);
		}
		for (TransactionId node : edges.keySet()){
			HashSet<TransactionId> neighbors = edges.get(node);
			neighbors.remove(tid);
		}
	}
	
	public boolean checkCycle(TransactionId t1, TransactionId t2){
		Queue<TransactionId> border = new LinkedList<TransactionId>();
		HashSet<TransactionId> visited = new HashSet<TransactionId>();
		border.add(t2);
		visited.add(t2);
		if ((t1==null)||(t2==null)){
			return false;
		}
		while (!border.isEmpty()){
			TransactionId current = border.poll();
			if (current.equals(t1)){
				return true;
			}
			if (!edges.containsKey(current)){
				continue;
			}
			for (TransactionId tid : edges.get(current)){
				if (!visited.contains(tid)){
					visited.add(tid);
					border.add(tid);
				}
			}
		}
		return false;
	} 
}
}

