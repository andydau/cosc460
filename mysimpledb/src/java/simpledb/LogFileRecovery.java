package simpledb;

import java.io.IOException;
import java.io.EOFException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * @author mhay
 */
class LogFileRecovery {

    private final RandomAccessFile readOnlyLog;

    /**
     * Helper class for LogFile during rollback and recovery.
     * This class given a read only view of the actual log file.
     *
     * If this class wants to modify the log, it should do something
     * like this:  Database.getLogFile().logAbort(tid);
     *
     * @param readOnlyLog a read only copy of the log file
     */
    public LogFileRecovery(RandomAccessFile readOnlyLog) {
        this.readOnlyLog = readOnlyLog;
    }

    /**
     * Print out a human readable representation of the log
     */
    public void print() throws IOException {
        // since we don't know when print will be called, we can save our current location in the file
        // and then jump back to it after printing
        Long currentOffset = readOnlyLog.getFilePointer();

        readOnlyLog.seek(0);
        long lastCheckpoint = readOnlyLog.readLong(); // ignore this
        System.out.println("BEGIN LOG FILE");
        while (readOnlyLog.getFilePointer() < readOnlyLog.length()) {
            int type = readOnlyLog.readInt();
            long tid = readOnlyLog.readLong();
            switch (type) {
                case LogType.BEGIN_RECORD:
                    System.out.println("<T_" + tid + " BEGIN>");
                    break;
                case LogType.COMMIT_RECORD:
                    System.out.println("<T_" + tid + " COMMIT>");
                    break;
                case LogType.ABORT_RECORD:
                    System.out.println("<T_" + tid + " ABORT>");
                    break;
                case LogType.UPDATE_RECORD:
                    Page beforeImg = LogFile.readPageData(readOnlyLog);
                    Page afterImg = LogFile.readPageData(readOnlyLog);  // after image
                    System.out.println("<T_" + tid + " UPDATE pid=" + beforeImg.getId() +">");
                    break;
                case LogType.CLR_RECORD:
                    afterImg = LogFile.readPageData(readOnlyLog);  // after image
                    System.out.println("<T_" + tid + " CLR pid=" + afterImg.getId() +">");
                    break;
                case LogType.CHECKPOINT_RECORD:
                    int count = readOnlyLog.readInt();
                    Set<Long> tids = new HashSet<Long>();
                    for (int i = 0; i < count; i++) {
                        long nextTid = readOnlyLog.readLong();
                        tids.add(nextTid);
                    }
                    System.out.println("<T_" + tid + " CHECKPOINT " + tids + ">");
                    break;
                default:
                    throw new RuntimeException("Unexpected type!  Type = " + type);
            }
            long startOfRecord = readOnlyLog.readLong();   // ignored, only useful when going backwards thru log
        }
        System.out.println("END LOG FILE");

        // return the file pointer to its original position
        readOnlyLog.seek(currentOffset);

    }

    /**
     * Rollback the specified transaction, setting the state of any
     * of pages it updated to their pre-updated state.  To preserve
     * transaction semantics, this should not be called on
     * transactions that have already committed (though this may not
     * be enforced by this method.)
     *
     * This is called from LogFile.recover after both the LogFile and
     * the BufferPool are locked.
     *
     * @param tidToRollback The transaction to rollback
     * @throws java.io.IOException if tidToRollback has already committed
     */
    public void rollback(TransactionId tidToRollback) throws IOException {
        readOnlyLog.seek(readOnlyLog.length()); // undoing so move to end of logfile

        // some code goes here
        long pointer = readOnlyLog.length();
        boolean begin = false;
        while (!begin){
        	pointer = pointer - LogFile.LONG_SIZE;
        	readOnlyLog.seek(pointer);
        	pointer = readOnlyLog.readLong();
        	readOnlyLog.seek(pointer);
        	int type = readOnlyLog.readInt();
        	if (type==LogType.BEGIN_RECORD){
        		long tid = readOnlyLog.readLong();
        		if (tid == tidToRollback.getId()){
        			begin = true;
        			continue;
        		}
        	}
        	if (type==LogType.COMMIT_RECORD){
        		long tid = readOnlyLog.readLong();
        		if (tid == tidToRollback.getId()){
        			throw new IOException();
        		}
        	}
        	if (type==LogType.UPDATE_RECORD){
        		long tid = readOnlyLog.readLong();
        		if (tid == tidToRollback.getId()){
        			Page before = Database.getLogFile().readPageData(readOnlyLog);
        			Page after = Database.getLogFile().readPageData(readOnlyLog);
        			int tableid = before.getId().getTableId();
        			HeapFile table = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
        			table.writePage(before);
        			Database.getLogFile().logCLR(tid, before);
        			Database.getBufferPool().removePage(after.getId());
        		}
        	}
        }
        Database.getLogFile().logAbort(tidToRollback.getId());;
    }

    /**
     * Recover the database system by ensuring that the updates of
     * committed transactions are installed and that the
     * updates of uncommitted transactions are not installed.
     *
     * This is called from LogFile.recover after both the LogFile and
     * the BufferPool are locked.
     */
    public void recover() throws IOException {
    	this.print();
    	readOnlyLog.seek(0);
    	long pointer = readOnlyLog.readLong();
    	ArrayList<Long> losers = new ArrayList<Long>();
    	if (pointer!=-1){
    		readOnlyLog.seek(pointer);
    		readOnlyLog.readInt();
    		readOnlyLog.readLong();
    		int noTransaction = readOnlyLog.readInt();
    		for (int i = 0; i < noTransaction; i++){
    			losers.add(readOnlyLog.readLong());
    		}
    		readOnlyLog.readLong();
    	}
    	while (true){
    		try{
    			int type = readOnlyLog.readInt();
    			if (type==LogType.BEGIN_RECORD){
    				long longid = readOnlyLog.readLong();
    				losers.add(longid);
    				
    			}
    			if (type==LogType.UPDATE_RECORD){
    				long tid = readOnlyLog.readLong();
    				Page before = Database.getLogFile().readPageData(readOnlyLog);
    				Page after = Database.getLogFile().readPageData(readOnlyLog);
    				int tableid = before.getId().getTableId();
    				HeapFile table = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
    				table.writePage(after);
    			}
    			if (type==LogType.CLR_RECORD){
    				long tid = readOnlyLog.readLong();
    				Page p = Database.getLogFile().readPageData(readOnlyLog);
    				int tableid = p.getId().getTableId();
    				HeapFile table = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
    				table.writePage(p);
    			}
    			if (type==LogType.COMMIT_RECORD){
    				long longid = readOnlyLog.readLong();
    				losers.remove(longid);
    			}
    			if (type==LogType.ABORT_RECORD){
    				long longid = readOnlyLog.readLong();
    				losers.remove(longid);
    			}
    			readOnlyLog.readLong();
    		}
    		catch (EOFException e){
    			System.out.println("End of log");
    			break;
    		}
    	}
    	readOnlyLog.seek(readOnlyLog.length());
    	pointer = readOnlyLog.length();
    	boolean begin = false;
    	ArrayList<Long> undone = new ArrayList<Long>();
        while (losers.size()>0){
        	pointer = pointer - LogFile.LONG_SIZE;
        	readOnlyLog.seek(pointer);
        	pointer = readOnlyLog.readLong();
        	readOnlyLog.seek(pointer);
        	int type = readOnlyLog.readInt();
        	if (type == LogType.CHECKPOINT_RECORD){
        		continue;
        	}
        	if (type==LogType.UPDATE_RECORD){
        		long tid = readOnlyLog.readLong();
        		if (losers.contains(tid)){
        			Page before = Database.getLogFile().readPageData(readOnlyLog);
        			Page after = Database.getLogFile().readPageData(readOnlyLog);
    				int tableid = before.getId().getTableId();
    				HeapFile table = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
    				table.writePage(before);
    				Database.getLogFile().logCLR(tid,before);
        		}
        	}
        	if (type==LogType.BEGIN_RECORD){
        		long tid = readOnlyLog.readLong();
        		if (losers.contains(tid)){
        			losers.remove(tid);
        			undone.add(tid);
        		}
        	}
        }
        for (Long tid : undone){
        	Database.getLogFile().logAbort(tid);
        }
    }
}
