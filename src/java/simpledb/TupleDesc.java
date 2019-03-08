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
        Type fieldType;

        /**
         * The name of the field
         * */
        String fieldName;

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
        //some code goes here
        return this.tdItems.iterator();
    }

    private static final long serialVersionUID = 1L;
    private Type[] typeAr = null;
    private String[] fieldAr = null;
    private ArrayList<TDItem> tdItems;

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
        this.typeAr = typeAr;
        this.fieldAr = fieldAr;
        this.tdItems = new ArrayList<>();
        for(int i = 0; i < typeAr.length; i++){
            this.tdItems.add(new TDItem(typeAr[i], fieldAr[i]));
        }
        // some code goes here
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
        this.typeAr = typeAr;
        // some code goes here
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.typeAr.length;
    }

    /**
     * Get all the types.
     * @return all the types.
     */
    public Type[] getTypes(){
        return this.typeAr;
    }

    /**
     * Get all the names of the fields.
     * @return all the names of the fields.
     */
    public String[] getFieldNames(){
        return this.fieldAr;
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
        if(this.fieldAr == null)
            return null;
        try{
            return this.fieldAr[i];
        }catch (NoSuchElementException e){
            System.out.println("not a valid field reference");
            throw e;
            //return null;
        }
        //return null;
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
        // some code goes here
        try{
            return this.typeAr[i];
        }catch (NoSuchElementException e){
            System.out.println("not a valid field reference");
            throw e;
        }
        //return null;
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
        // some code goes here
        if(this.fieldAr == null) throw new NoSuchElementException();
        for(int i = 0; i < this.fieldAr.length; i++){
            if(this.fieldAr[i].equals(name))
                return i;
        }


        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size = 0;
        for(Type t : this.typeAr){
            size += t.getLen();
        }
        return size;
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
        // some code goes here
        Type[] resType = new Type[td1.numFields() + td2.numFields()];
        String[] resField = new String[td1.numFields() + td2.numFields()];
        System.arraycopy(td1.getTypes(), 0, resType, 0, td1.numFields());
        System.arraycopy(td2.getTypes(), 0, resType, td1.numFields(), td2.numFields());
        System.arraycopy(td1.getFieldNames(), 0, resField, 0, td1.numFields());
        System.arraycopy(td2.getFieldNames(), 0, resField, td1.numFields(), td2.numFields());
        return new TupleDesc(resType, resField);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
        if(this == o) return true;
        if(!(o instanceof TupleDesc)) return false;
        TupleDesc other = (TupleDesc) o;
        if(this.getSize() != other.getSize()) return false;
        Type[] tmp = other.getTypes();
        for(int i = 0; i < this.typeAr.length; i++){
            if(!this.typeAr[i].equals(tmp[i])) return false;
        }
        return true;
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
        // some code goes here
        String s = "";
        if(this.fieldAr == null){
            for(int i = 0; i < this.typeAr.length; i++){
                if(i == this.typeAr.length - 1) s = s + typeAr[i].toString();
                else s = s + this.typeAr[i].toString() + ",";
            }
        }
        else{
            for(int i = 0; i < this.typeAr.length; i++){
                if(i == this.typeAr.length - 1)
                    s = s + this.typeAr[i].toString() + "(" + this.fieldAr[i] + ")";
                else
                    s = s + this.typeAr[i].toString() + "(" + this.fieldAr[i] + ")" + ",";
            }
        }
        return s;
    }


}
