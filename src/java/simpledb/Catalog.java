package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import simpledb.TestUtil.SkeletonFile;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 * 
 * @Threadsafe
 */
public class Catalog {

	private static int id; // unique identifier
	ArrayList<Tables> tables;
	
	// a set of information about a table
	private class Tables {
		public String tableName;
		public final DbFile file;
		public final String primaryField;
		public final int index;
		public boolean dupName;
		
		{
			dupName = false;
		}
		
		Tables(DbFile file, String name, String pf, int i) {
			this.tableName = name;
			this.file = file;
			this.primaryField = pf;
			this.index = i;
		
		}
	}
    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
        id = 0;
    	tables = new ArrayList<>();
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * conflict exists, use the last table to be added as the table for a given name.
     * @param pkeyField the name of the primary key field
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        
    	int NameConflict = 0;
    	int length = 0;
    	Tables cf = null;
    	
    	if (name == null)
        	name = "NULL";
    	//System.out.println(file.);
        for (Tables t: tables) 
        {
        	length = t.tableName.length() < name.length() ? t.tableName.length() : name.length();
        	if (t.tableName.subSequence(0, length).equals(name)) {
        		NameConflict++;
        		if (!t.dupName)
        			cf = t;
        	}
        }
        
        if (cf != null) 
        {
        	cf.dupName = true;
        	cf.tableName += String.format("#%d", NameConflict);
        }
        
        
        tables.add(new Tables(file, name, pkeyField, id));
        id++;
    	
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        for (Tables t: tables)
        	if (t.tableName.equals(name))
        		return t.file.getId();
        
        throw new NoSuchElementException();
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
    	for (Tables t: tables)
        	if (t.file.getId() == tableid)
        		return t.file.getTupleDesc();
    	throw new NoSuchElementException();
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
    	for (Tables t: tables)
        	if (t.file.getId() == tableid)
        		return t.file;
        throw new NoSuchElementException();
    }

    public String getPrimaryKey(int tableid) {
    	for (Tables t: tables)
        	if (t.file.getId() == tableid)
        		return t.primaryField;
        throw new NoSuchElementException();
    }

    public Iterator<Integer> tableIdIterator() {
        // some code goes here
        return null;
    }

    public String getTableName(int id) {
    	for (Tables t: tables)
        	if (t.file.getId() == id)
        		return t.tableName;
        throw new NoSuchElementException();
    }
    
    /** Delete all tables from the catalog */
    public void clear() {
        // some code goes here
    	tables.clear();
    	id = 0; //?
    }
    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder=new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));
            
            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder+"/"+name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
    
    public static void main(String[] args) {
    	DbFile f = new SkeletonFile(0, Utility.getTupleDesc(2));
    	DbFile f1 = new SkeletonFile(0, Utility.getTupleDesc(2));
//    	DbFile f2 = new SkeletonFile(2, Utility.getTupleDesc(2));
//    	
    	
    	System.out.println(f.getId());
    	System.out.println(f1.getId());
    	
    	Database.getCatalog().addTable(f, "name");
    	Database.getCatalog().addTable(f1, "name5");
    	
//    	Database.getCatalog().addTable(f2, "name");
//    	
    	System.out.println(Database.getCatalog().getTableName(0));
    	System.out.println(Database.getCatalog().getTableName(0));
    }
}

