package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return new Itr();
    }

    private class Itr implements Iterator<TDItem> {
    	private int index;
    
		@Override
		public boolean hasNext() {
			// TODO Auto-generated method stub
			return index != fieldNum;
		}

		@Override
		public TDItem next() {
			// TODO Auto-generated method stub
			return item[index++];
		}
    	
    }
    
    private static final long serialVersionUID = 1L;
    //
    private TDItem[] item;
    private final int fieldNum;
    // static meaning there is only one in the memory.
    private final static String strnull = "NULL";
    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
    	// done in lab1.ex1
    	fieldNum = typeAr.length;
    	item = new TDItem[fieldNum];
    	
    	if (typeAr.length < 1)
    		return;
    	if (fieldAr.length != typeAr.length)
    		return;
    	
    	for(int i = 0; i < fieldNum; i++) {
    		item[i] = new TDItem(typeAr[i], fieldAr[i]);
    	}
    	
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
    	// done in lab1.ex1
    	fieldNum = typeAr.length;
    	item = new TDItem[fieldNum];
    	
    	if (typeAr.length < 1)
    		return;
 
    	for(int i = 0; i < fieldNum; i++) {
    		item[i] = new TDItem(typeAr[i], "NULL");
    	}
    	
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return fieldNum;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
    	if (i > fieldNum)
    		throw new NoSuchElementException();
    	
        return item[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
    	
    	if (i > fieldNum)
    		throw new NoSuchElementException();
    	
        return item[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
    	if (name == null)
    		throw new  NoSuchElementException();
    	
        for (int i = 0; i < fieldNum; i++) {
        	if (name.equals(item[i].fieldName))
        		return i;
        }
       throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int sum = 0;
    	for (int i = 0; i < fieldNum; i++) {
        	sum += item[i].fieldType.getLen();
        }
        return sum;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        int num = td1.numFields() + td2.numFields();
        Type[] typeAr = new Type[num];
        String[] nameAr = new String[num];
        for (int i = 0; i < td1.numFields(); i++) {
        	typeAr[i] = td1.getFieldType(i);
        	nameAr[i] = td1.getFieldName(i);
        }
        for (int i = 0; i < td2.numFields(); i++) {
        	typeAr[i+td1.numFields()] = td2.getFieldType(i);
        	nameAr[i+td1.numFields()] = td2.getFieldName(i);
        }
        return new TupleDesc(typeAr, nameAr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
    	
    	// see core java 10th P.167 about
    	// the method equals
        if (this == o)
        	return true;
        if (o == null)
        	return false;
        if (getClass() != o.getClass())
        	return false;
        
        TupleDesc other = (TupleDesc) o;
        if (other.numFields() != this.fieldNum)
        	return false;
        
        for (int i = 0; i < fieldNum; i++)
        	if (this.item[i].fieldType !=
        			other.getFieldType(i))
        		return false;
        
        return this.fieldNum == other.numFields(); 
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {

    	String out = new String();
    	for (int i = 0; i < fieldNum; i++) 
    		out += item[i].toString() + '[' + i + "] ";
        return out;
    }
    
    
    /**
     *  unit test
     */
    public static void main(String[] args) {
        Type types[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE };
        String names[] = new String[]{ "field0", "field1", "field2" };
        TupleDesc descriptor = new TupleDesc(types, names);
        
        Type types2[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE };
        String names2[] = new String[]{ "field3", "field4", "field5" };
        TupleDesc descriptor2 = new TupleDesc(types2, names2);
        
        System.out.println("equal ? " + descriptor.equals(descriptor2));
        
        descriptor = TupleDesc.merge(descriptor, descriptor2);
        
        
        System.out.println(descriptor.toString());
//        Iterator itr = descriptor.iterator();
//        while (itr.hasNext())
//        	System.out.println(itr.next().toString());
    }
}
