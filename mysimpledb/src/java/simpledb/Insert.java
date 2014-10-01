package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     *
     * @param t       The transaction running the insert.
     * @param child   The child operator from which to read tuples to be inserted.
     * @param tableid The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to
     *                     insert.
     */
    private DbIterator child;
    private int tableId;
    private TransactionId tid;
    private boolean done = false;
    public Insert(TransactionId t, DbIterator child, int tableid)
            throws DbException {
        this.child = child;
        this.tid = t;
        this.tableId = tableid;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
    	Type[] arr = {Type.INT_TYPE};
        return new TupleDesc(arr);
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        this.child.open();
    }

    public void close() {
        this.child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
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
    			bp.insertTuple(this.tid, this.tableId, temp);
    			count++;
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
