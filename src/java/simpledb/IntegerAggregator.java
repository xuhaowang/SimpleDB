package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private HashMap<Field, Integer> index = new HashMap<>();
    private HashMap<Field, Integer> counter = new HashMap<>();
    private ArrayList<Tuple> res = new ArrayList<>();
    private ArrayList<Tuple> avgRes = new ArrayList<>();
    private boolean noGrouping = false;
    private TupleDesc td;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        if(gbfield == Aggregator.NO_GROUPING)
            this.noGrouping = true;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        if(noGrouping)
            this.td = new TupleDesc(new Type[]{Type.INT_TYPE});
        else
            this.td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});

    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Tuple newTuple;
        Tuple t2 = new Tuple(td);
        Field af = tup.getField(afield);
        Field gf;
        if(!noGrouping){
            gf = tup.getField(gbfield);
            t2.setField(0, gf);
            t2.setField(1, af);
            if(!index.containsKey(gf)){
                counter.put(gf, 1);
                index.put(gf, res.size());
                if(what == Op.COUNT)
                    t2.setField(1, new IntField(1));
                res.add(t2);
                avgRes.add(t2);
            }
            else {
                int i = index.get(gf);
                Tuple t1 = res.get(i);
                newTuple = mergeTuples(t1, t2, gf);
                res.set(i, newTuple);
                int c = counter.get(gf);
                counter.put(gf, c + 1);
            }
        }
        else {
            gf = new IntField(Aggregator.NO_GROUPING);
            t2.setField(0, af);
            index.put(gf, 0);
            if(res.size() == 0){
                res.add(t2);
                avgRes.add(t2);
                counter.put(gf, 1);
            }
            else {
                Tuple t1 = res.get(0);
                newTuple = mergeTuples(t1, t2, gf);
                res.set(0, newTuple);
                int c = counter.get(gf);
                counter.put(gf, c + 1);
            }
        }
        if(what == Op.AVG){
            int abfield = 1;
            if(noGrouping)
                abfield = 0;
            else
                abfield = 1;
            for(Map.Entry<Field, Integer> e: counter.entrySet()){
                Field key = e.getKey();
                Integer value = e.getValue();
                Tuple originTup = res.get(index.get(key));
                Tuple newTup = new Tuple(td);
                if(abfield == 1){
                    gf = originTup.getField(0);
                    newTup.setField(0, gf);
                }
                IntField originField = (IntField) originTup.getField(abfield);
                int sum = originField.getValue();
                int r = sum / value;
                IntField newField = new IntField(r);
                newTup.setField(abfield, newField);
                avgRes.set(index.get(key), newTup);
            }
        }

    }

    /**
     *
     * @param tuple1
     * @param tuple2
     * @param gf
     * @return Merge two tuples using the specified operator op
     */
    private Tuple mergeTuples(Tuple tuple1, Tuple tuple2, Field gf){
        Tuple newTuple = new Tuple(td);
        int afield = 1;
        if(noGrouping)
            afield = 0;
        else{
            afield = 1;
            newTuple.setField(0, gf);
        }
        IntField f1 = (IntField)tuple1.getField(afield);
        IntField f2 = (IntField)tuple2.getField(afield);
        int v1 = f1.getValue();
        int v2 = f2.getValue();
        switch (what){
            case MIN:
                if(v1 < v2)
                    return tuple1;
                else
                    return tuple2;
            case MAX:
                if(v1 < v2)
                    return tuple2;
                else
                    return tuple1;
            case AVG:
            case SUM:
                int v = v1 + v2;
                newTuple.setField(afield, new IntField(v));
                return newTuple;
            case COUNT:
                newTuple.setField(afield, new IntField(counter.get(gf) + 1));
                return newTuple;

           /* case AVG:
                int s = v1 * counter.get(gf) + v2;
                int c = counter.get(gf) + 1;
                int r = s / c;
                newTuple.setField(afield, new IntField(r));
                return newTuple;*/
            default:
                throw new IllegalArgumentException("Unsupported operation " + what);
        }

        //return null;
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        if(what != Op.AVG)
            return new MyIntAggIterator(td, res);
        return new MyIntAggIterator(td, avgRes);
    }

    private class MyIntAggIterator implements DbIterator{
        Iterator<Tuple> i = null;
        TupleDesc td = null;
        Iterable<Tuple> tuples = null;
        public MyIntAggIterator(TupleDesc td, Iterable<Tuple> tuples){
            this.td = td;
            this.tuples = tuples;
            for (Tuple t : tuples) {
                if (!t.getTupleDesc().equals(td))
                    throw new IllegalArgumentException(
                            "incompatible tuple in tuple set");
            }
        }

        public void open() {
            i = tuples.iterator();
        }

        public boolean hasNext() {
            return i.hasNext();
        }

        public Tuple next() {
            return i.next();
        }

        public void rewind() {
            close();
            open();
        }
        public TupleDesc getTupleDesc() {
            return td;
        }

        public void close() {
            i = null;
        }

    }

}
