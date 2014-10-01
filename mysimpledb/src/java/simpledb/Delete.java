package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t     The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    private TransactionId tid;
    private DbIterator child;
    private boolean done = false;
    public Delete(TransactionId t, DbIterator child) {
        this.tid = t;
        this.child = child;
    }

    public TupleDesc getTupleDesc() {
        // some code goes null;
    	Type[] arr = {Type.INT_TYPE};
        return new TupleDesc(arr);
    }

    public void open() throws DbException, TransactionAbortedException {
    	super.open();
        this.child.open();
    }

    public void close() {
    	super.close();
        this.child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.child.rewind();
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
        // some code goes here
    	if (this.done){
    		return null;
    	};
        BufferPool bp = Database.getBufferPool();
        int count = 0;
        while (this.child.hasNext()){
        	Tuple temp = this.child.next();
        	try{
        		bp.deleteTuple(this.tid, temp);
        		count++;
        	}
        	catch (DbException e){
        		continue;
        	}
        	catch (IOException e){
        		e.printStackTrace();
        	}
        }
        Field f = new IntField(count);
    	Type[] arr = {Type.INT_TYPE};
    	TupleDesc td = new TupleDesc(arr);
    	Tuple result = new Tuple(td);
    	result.setField(0, f);
    	this.done = true;
        return result;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
    	DbIterator[] result = new DbIterator[1];
        result[0] = this.child;
        return result;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        this.child = children[0];
    }

}
