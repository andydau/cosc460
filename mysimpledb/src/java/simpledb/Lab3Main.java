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
        SeqScan scanProfs = new SeqScan(tid, Database.getCatalog().getTableId("profs"));
        StringField hay = new StringField("hay", Type.STRING_LEN);
        Predicate p = new Predicate(1, Predicate.Op.EQUALS, hay);
        Filter filterProfs = new Filter(p, scanProfs);
        
        SeqScan scanTakes = new SeqScan(tid, Database.getCatalog().getTableId("takes"));
        
        JoinPredicate jp = new JoinPredicate(1, Predicate.Op.EQUALS, 2);
        Join join1 = new Join(jp, scanTakes, filterProfs);

        SeqScan scanStudents = new SeqScan(tid, Database.getCatalog().getTableId("students"));
        jp = new JoinPredicate(0, Predicate.Op.EQUALS, 0);
        Join join2 = new Join(jp, scanStudents, join1);
        // query execution: we open the iterator of the root and iterate through results
        System.out.println("Query results:");
        join2.open();
        while (join2.hasNext()) {
            Tuple tup = join2.next();
            System.out.println("\t"+tup.getField(1));
        }
        join2.close();
        Database.getBufferPool().transactionComplete(tid);
    }

}