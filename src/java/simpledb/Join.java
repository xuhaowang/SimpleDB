package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;
    private JoinPredicate p;
    private DbIterator child1;
    private DbIterator child2;
    private TupleDesc td1;
    private TupleDesc td2;
    private TupleDesc td;
    private Tuple cursor;
    int i = 0;

    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, DbIterator child1, DbIterator child2) {
        // some code goes here
        this.p = p;
        this.child1 = child1;
        this.child2 = child2;
        this.td1 = child1.getTupleDesc();
        this.td2 = child2.getTupleDesc();
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return p;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here
        int field1 = p.getField1();
        return td1.getFieldName(field1);
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        int field2 = p.getField2();
        return td2.getFieldName(field2);
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        int numFileds = td1.numFields() + td2.numFields();
        Type[] typeAr = new Type[numFileds];
        String[] fieldAr = new String[numFileds];
        Type[] td1TypeAr = td1.getTypes();
        Type[] td2TypeAr = td2.getTypes();
        String[] td1FieldAr = td1.getFieldNames();
        String[] td2FieldAr = td2.getFieldNames();
        System.arraycopy(td1TypeAr, 0, typeAr, 0, td1.numFields());
        System.arraycopy(td2TypeAr, 0, typeAr, td1.numFields(), td2.numFields());
        if(td1FieldAr != null && td2FieldAr != null){
            System.arraycopy(td1FieldAr, 0, fieldAr, 0, td1.numFields());
            System.arraycopy(td2FieldAr, 0, fieldAr, td1.numFields(), td2.numFields());
        }
        this.td = new TupleDesc(typeAr, fieldAr);
        return this.td;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        child1.open();
        child2.open();
        super.open();
    }

    public void close() {
        // some code goes here
        super.close();
        child1.close();
        child2.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child1.rewind();
        child2.rewind();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(td == null)
            td = getTupleDesc();
        Tuple newTuple = new Tuple(td);
        boolean flag = true;
        while (child1.hasNext() || child2.hasNext()){
            Tuple t1 ;
            if(cursor != null && flag){
                t1 = cursor;
                flag = false;
            }
            else{
                t1 = child1.next();
                cursor = t1;
                flag = false;
            }
            if(t1 == null)
                break;

            while (child2.hasNext()){
                i++;
                Tuple t2 = child2.next();
                if(p.filter(t1, t2)){
                    for(int i = 0; i < td1.numFields(); i++)
                        newTuple.setField(i, t1.getField(i));
                    for(int i = td1.numFields(); i < td.numFields(); i++)
                        newTuple.setField(i, t2.getField(i - td1.numFields()));
                    return newTuple;
                }
            }
            child2.rewind();
        }
        return null;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[] { this.child1, this.child2 };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        if (this.child1!=children[0])
        {
            this.child1 = children[0];
        }
        if (this.child2!=children[1])
        {
            this.child2 = children[1];
        }
    }

}
