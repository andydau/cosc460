package simpledb;

import java.io.IOException;

public class Lab3Main {

    public static void main(String[] argv) 
       throws DbException, TransactionAbortedException, IOException {

        System.out.println("Loading schema from file:");
        // file named college.schema must be in mysimpledb directory
        Database.getCatalog().loadSchema("college.schema");

        // SQL query: SELECT * FROM STUDENTS WHERE name="Alice"
        // algebra translation: select_{name="alice"}( Students )
        // query plan: a tree with the following structure
        // - a Filter operator is the root; filter keeps only those w/ name=Alice
        // - a SeqScan operator on Students at the child of root
        TransactionId tid = new TransactionId();
        SeqScan scanCourses = new SeqScan(tid, Database.getCatalog().getTableId("students"));
        SeqScan scanProfs = new SeqScan(tid, Database.getCatalog().getTableId("takes"));
        JoinPredicate p = new JoinPredicate(0, Predicate.Op.EQUALS, 0);
        Join joinTest = new Join(p, scanCourses, scanProfs);

        // query execution: we open the iterator of the root and iterate through results
        System.out.println("Query results:");
        joinTest.open();
        while (joinTest.hasNext()) {
            Tuple tup = joinTest.next();
            System.out.println("\t"+tup);
        }
        joinTest.close();
        Database.getBufferPool().transactionComplete(tid);
    }

}