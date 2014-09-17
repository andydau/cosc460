package simpledb;
import java.io.*;

public class Lab2Main {

    public static void main(String[] argv) {

        // construct a 3-column table schema
        Type types[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE };
        String names[] = new String[]{ "field0", "field1", "field2" };
        TupleDesc descriptor = new TupleDesc(types, names);

        // create the table, associate it with some_data_file.dat
        // and tell the catalog about the schema of this table.
        HeapFile table1 = new HeapFile(new File("some_data_file.dat"), descriptor);
        Database.getCatalog().addTable(table1, "test");

        // construct the query: we use a simple SeqScan, which spoonfeeds
        // tuples via its iterator.
        TransactionId tid = new TransactionId();
        SeqScan f = new SeqScan(tid, table1.getId());

        try {
            // and run it
            f.open();
            
            //check field1 and modify is necessary
            while (f.hasNext()) {
                Tuple tup = f.next();
                IntField field1 = (IntField) tup.getField(1);
                int value = field1.getValue();
                if (value<3){
                	Tuple newTup = new Tuple(f.getTupleDesc());
                	Field comp = new IntField(3);
                	newTup.setField(0, tup.getField(0));
                	newTup.setField(1, comp);
                	newTup.setField(2, tup.getField(2));
                	table1.deleteTuple(tid, tup);
                	table1.insertTuple(tid, newTup);
                }
            }
            
            //Add the last tuple
            Tuple newTup = new Tuple(f.getTupleDesc());
            Field newF = new IntField(99);
            newTup.setField(0, newF);
        	newTup.setField(1, newF);
        	newTup.setField(2, newF);
        	table1.insertTuple(tid, newTup);
        	
        	Database.getBufferPool().flushAllPages();
        	f.open();
        	while (f.hasNext()) {
                Tuple tup = f.next();
                System.out.println(tup);
            }
            f.close();
            Database.getBufferPool().transactionComplete(tid);
        } catch (Exception e) {
            System.out.println ("Exception : " + e);
        }
    }

}