package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    //
    private final int numFields;
    private TupleDesc tupleDescriptor;
    private RecordId rid;
    private Field[] fields;
    
    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
    	
    	numFields = td.numFields();
    	if (numFields < 1)
        	return;
    	tupleDescriptor = td;
    	fields = new Field[numFields];
//    	for (int i = 0; i < numFields; i++) {
//    		if (td.getFieldType(i) == Type.INT_TYPE)
//    			;
//    		else if (td.getFieldType(i) == Type.STRING_TYPE)
//    			;
//    		else 
//    			fields[i] = null;
    	}
    

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDescriptor;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return rid;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
    	this.rid = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
    	if (i > numFields) 
    		return;
    	if (tupleDescriptor.getFieldType(i) != f.getType())
    		return;
        fields[i] = f; 
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
    	if (i > numFields)
    		return null;
        return fields[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // some code goes here
    	String str = new String();
    	for (int i = 0; i < numFields; i++)
    		str += String.format("%s ", fields[i].toString());
    	return str;
        //throw new UnsupportedOperationException("Implement this");
    	
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
        return null;
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        tupleDescriptor = td;
    }
    
    
    public static void main(String[] args) {
        Type types[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE};
        String names[] = new String[]{ "field0", "field1" };
        TupleDesc descriptor = new TupleDesc(types, names);
        
        Tuple tup = new Tuple(descriptor);
        tup.setField(0, new IntField(-1));
        tup.setField(1, new StringField("abc", 128));
        
        
        System.out.println(tup.toString());

    }
}
