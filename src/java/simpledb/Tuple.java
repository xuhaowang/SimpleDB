package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.NoSuchElementException;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    private TupleDesc td;
    private Field[] field;
    private RecordId recordId;
    protected transient int modCount = 0;
    private int size = 0;

    /**
     * Create a new tuple with the specified schema (type).
     * 
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
        this.td = td;
        this.size = td.numFields();
        this.field = new Field[td.numFields()];
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return this.recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     * 
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        this.recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     * 
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) throws ArrayIndexOutOfBoundsException{
        // some code goes here
        if(i < 0 || i >= this.td.numFields()){
            throw new ArrayIndexOutOfBoundsException("invaild index");
        }
        this.field[i] = f;

    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     * 
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) throws ArrayIndexOutOfBoundsException{
        // some code goes here
        if(i < 0 || i >= this.td.numFields()){
            throw new ArrayIndexOutOfBoundsException("invaild index");
        }
        return this.field[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * 
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     * 
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() {
        // some code goes here
        StringBuilder s = new StringBuilder();
        for(Field f : this.field){
            s.append(f.toString() + " ");
        }
        s.append("\n");
        return s.toString();

        //throw new UnsupportedOperationException("Implement this");
    }
    
    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
        return new MyIterator();
    }

    private class MyIterator implements Iterator<Field>{
        int cursor = 0;
        int lastRet = -1;
        int expectedModCount;

        MyIterator(){
            this.expectedModCount = Tuple.this.modCount;
        }

        public boolean hasNext(){
            return this.cursor != Tuple.this.size;
        }

        public Field next(){
            this.checkForComodification();
            int i = this.cursor;
            if (i >= Tuple.this.size){
                throw new NoSuchElementException();
            }
            else {
                Field[] elementData = Tuple.this.field;
                if (i >= elementData.length) {
                    throw new ConcurrentModificationException();
                } else {
                    this.cursor = i + 1;
                    return elementData[this.lastRet = i];
                }
            }

        }

        public void remove(){
            //some code
        }

        final void checkForComodification(){
            if (this.expectedModCount != Tuple.this.modCount){
                throw new ConcurrentModificationException();
            }
        }

    }
}
