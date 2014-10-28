package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import simpledb.Predicate.Op;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 * <p/>
 * This class is not needed in implementing lab1|lab2|lab3.                                                   // cosc460
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }

    public static void setStatsMap(HashMap<String, TableStats> s) {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     *
     * @param tableid       The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO. This doesn't differentiate between
     *                      sequential-scan IO and disk seeks.
     */
    private int ioCostPerPage;
    private int pageNum;
    private int tupleNum;
    private HeapFile table;
    private HashMap<Integer,HashMap<Field,Integer>> map;
    private HashMap<Integer,Object> histoMap;
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
    	this.ioCostPerPage = ioCostPerPage;
    	TransactionId tid = new TransactionId();
    	this.table = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
    	DbFileIterator tupleIt = table.iterator(tid);
    	map = new HashMap<Integer,HashMap<Field,Integer>>();
    	histoMap = new HashMap<Integer,Object>();
    	HashMap<Integer,Field> maxMap = new HashMap<Integer,Field>();
    	HashMap<Integer,Field> minMap = new HashMap<Integer,Field>();
    	this.tupleNum = 0;
    	try{
    		tupleIt.open();
    		TupleDesc td = this.table.getTupleDesc();
    		while (tupleIt.hasNext()){
    			Tuple temp = tupleIt.next();
    			int numField = td.numFields();
    			for (int i=0; i < numField;i++){
    				Field f = temp.getField(i);
    				if (!map.containsKey(i)){
    					HashMap<Field,Integer> newMap = new HashMap<Field,Integer>();
    					map.put(i, newMap);
    				}
    				if (!maxMap.containsKey(i)){
    					maxMap.put(i, f);
    					minMap.put(i, f);
    				}
    				else{
    					if (f.compare(Op.GREATER_THAN, maxMap.get(i))){
    						maxMap.put(i, f);
    					}
    					if (f.compare(Op.LESS_THAN, minMap.get(i))){
    						minMap.put(i, f);
    					}
    				}
    				map.get(i).put(f, 1);
    			}
    			this.tupleNum++;
    		}
    		for (int i=0; i < td.numFields();i++){
    			Type t = td.getFieldType(i);
    			if (t==Type.INT_TYPE){
    				int max = ((IntField)maxMap.get(i)).getValue();
    				int min = ((IntField)minMap.get(i)).getValue();
    				IntHistogram h = new IntHistogram(NUM_HIST_BINS,min,max);
    				histoMap.put(i, h);
    			}
    			if (t==Type.STRING_TYPE){
    				StringHistogram h = new StringHistogram(NUM_HIST_BINS);
    				histoMap.put(i, h);
    			}
    		}
    		tupleIt.rewind();
    		while (tupleIt.hasNext()){
    			Tuple temp = tupleIt.next();
    			int numField = td.numFields();
    			for (int i=0; i < numField;i++){
    				Field f = temp.getField(i);
    				Type t = td.getFieldType(i);
        			if (t==Type.INT_TYPE){
        				IntHistogram h = (IntHistogram)histoMap.get(i);
        				h.addValue(((IntField)f).getValue());
        			}
        			if (t==Type.STRING_TYPE){
        				StringHistogram h = (StringHistogram)histoMap.get(i);
        				h.addValue(((StringField)f).getValue());
        			}
    			}
    		}
    	}
    	catch (Exception e){
    		e.printStackTrace();
    	}
    	this.pageNum = table.numPages();
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * <p/>
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     *
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return this.pageNum*this.ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     * selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int)(this.tupleNum*selectivityFactor);
    }

    /**
     * This method returns the number of distinct values for a given field.
     * If the field is a primary key of the table, then the number of distinct
     * values is equal to the number of tuples.  If the field is not a primary key
     * then this must be explicitly calculated.  Note: these calculations should
     * be done once in the constructor and not each time this method is called. In
     * addition, it should only require space linear in the number of distinct values
     * which may be much less than the number of values.
     *
     * @param field the index of the field
     * @return The number of distinct values of the field.
     */
    public int numDistinctValues(int field) {
        // some code goes here
        return this.map.get(field).size();
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     *
     * @param field    The field over which the predicate ranges
     * @param op       The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     * predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
    	TupleDesc td = this.table.getTupleDesc();
    	Type t = td.getFieldType(field);
    	if (t==Type.INT_TYPE){
    		return ((IntHistogram)histoMap.get(field)).estimateSelectivity(op,((IntField)constant).getValue());
    	}
    	if (t==Type.STRING_TYPE){
    		return ((StringHistogram)histoMap.get(field)).estimateSelectivity(op,((StringField)constant).getValue());
    	}
    	return 0;
    }

}
