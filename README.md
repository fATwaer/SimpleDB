simple-db
=========

a simple instance
``` Java
package simpledb;
import java.io.*;

public class jointest {

    public static void main(String[] argv) {
        // construct a 3-column table schema
        Type types[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE };
        String names[] = new String[]{ "field0", "field1", "field2" };

        TupleDesc td = new TupleDesc(types, names);

        // create the tables, associate them with the data files
        // and tell the catalog about the schema  the tables.
        HeapFile table1 = new HeapFile(new File("some_data_file1.dat"), td);
        Database.getCatalog().addTable(table1, "t1");

        HeapFile table2 = new HeapFile(new File("some_data_file2.dat"), td);
        Database.getCatalog().addTable(table2, "t2");

        // construct the query: we use two SeqScans, which spoonfeed
        // tuples via iterators into join
        TransactionId tid = new TransactionId();

        SeqScan ss1 = new SeqScan(tid, table1.getId(), "t1");
        SeqScan ss2 = new SeqScan(tid, table2.getId(), "t2");

        // create a filter for the where condition
        Filter sf1 = new Filter(
                                new Predicate(0,
                                Predicate.Op.GREATER_THAN, new IntField(1)),  ss1);

        JoinPredicate p = new JoinPredicate(1, Predicate.Op.EQUALS, 1);
        Join j = new Join(p, sf1, ss2);

        // and run it
        try {
            j.open();
            while (j.hasNext()) {
                Tuple tup = j.next();
                System.out.println(tup);
            }
            j.close();
            Database.getBufferPool().transactionComplete(tid);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
```



Output Test
---
	
	java -jar dist/simpledb.jar parser catalog.txt

to run the database.

``` bash

SimpleDB> select * from data d;
Started a new transaction tid = 2
Added scan of table d
Added select list field null.*
The query plan is:
  π(d.f1,d.f2),card:0
  |
scan(data d)

d.f1	d.f2	
------------------
0
1 10 
2 20 
3 30 
4 40 
5 50 
6 60 
5 50 

 7 rows.
Transaction 2 committed.
```
