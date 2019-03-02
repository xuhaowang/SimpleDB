package simpledb;

import java.io.*;
import java.nio.channels.FileChannel;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    private File f;
    private TupleDesc td;
    private String name;
    private int numPages;
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
        this.numPages = (int)f.length() / BufferPool.PAGE_SIZE;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
        //throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        try {
            RandomAccessFile raf = new RandomAccessFile(this.f, "r");
            long pos = BufferPool.PAGE_SIZE * pid.pageNumber();
            byte[] data = new byte[BufferPool.PAGE_SIZE];
            raf.seek(pos);
            raf.read(data, 0, BufferPool.PAGE_SIZE);
            HeapPage hp = new HeapPage((HeapPageId)pid, data);
            return hp;

        }catch (IOException e){
            System.out.println("File error");
            return null;
        }

    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        HeapPage hp = (HeapPage)page;
        int pgNo = hp.getId().pageNumber();
        RandomAccessFile raf = new RandomAccessFile(this.f, "rw");
        byte[] data = hp.getPageData();
        raf.seek(pgNo * BufferPool.PAGE_SIZE);
        raf.write(data);

    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here

        return this.numPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        if(!t.getTupleDesc().equals(this.td))
            throw new DbException("Wrong TupleDesc!");
        ArrayList<Page> res = new ArrayList<>();
        int nPages = this.numPages();
        int tableId = this.getId();
        Permissions perm = Permissions.READ_WRITE;
        //int headerSize = 0;
        for(int i = 0; i < nPages; i++){
            HeapPageId hpid = new HeapPageId(tableId, i);
            HeapPage hp = (HeapPage)Database.getBufferPool().getPage(tid, hpid, perm);
            //headerSize = hp.getHeaderSize();
            if(hp.getNumEmptySlots() > 0){
                hp.insertTuple(t);
                res.add(hp);
            //    writePage(hp);
                break;
            }
        }
        if(res.isEmpty()){
            HeapPageId hpid = new HeapPageId(tableId, nPages);
            byte[] initData = new byte[BufferPool.PAGE_SIZE];
            HeapPage newHp = new HeapPage(hpid, initData);
            newHp.insertTuple(t);
            res.add(newHp);
            this.numPages++;
            writePage(newHp);
        }
        return res;

    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException, IOException {
        // some code goes here
        PageId pid = t.getRecordId().getPageId();
        Permissions perm = Permissions.READ_WRITE;
        HeapPage hp = (HeapPage)Database.getBufferPool().getPage(tid, pid, perm);
        hp.deleteTuple(t);
        if(hp.isEmpty()){
            RandomAccessFile raf = new RandomAccessFile(this.f, "rw");
            int pgNo = hp.getId().pageNumber();
            int tableId = this.getId();
            FileChannel fc = raf.getChannel();
            for(int i = pgNo + 1; i < this.numPages; i++){
                HeapPageId hpid = new HeapPageId(tableId, i);
                HeapPage hpi = (HeapPage)Database.getBufferPool().getPage(tid, hpid, perm);
                byte[] data = hpi.getPageData();
                raf.seek(i * BufferPool.PAGE_SIZE);
                raf.write(data);
            }
            fc.truncate(this.numPages  * BufferPool.PAGE_SIZE);
            this.numPages--;
            fc.close();
            //writePage(hp);
        }
        return hp;

    }



    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new MyIterator(tid);
    }

    private class MyIterator implements DbFileIterator{
        private boolean open = false;
        private int tableId;
        private TransactionId tid;
        private int numPages;
        private int currentPgNo = 0;
        private Permissions perm;
        private HeapPage pg;
        private Iterator<Tuple> pgItr;
        public MyIterator(TransactionId tid){
            this.tableId = HeapFile.this.getId();
            this.tid = tid;
            this.numPages = HeapFile.this.numPages();
            this.perm = Permissions.READ_ONLY;
        }
        public void open() throws DbException, TransactionAbortedException{
            HeapPageId pid;
            try {
                open = true;
                pid = new HeapPageId(this.tableId, this.currentPgNo);
                this.pg = (HeapPage) Database.getBufferPool().getPage(this.tid, pid, this.perm);
                this.pgItr = this.pg.iterator();
            }catch (IOException e){
                throw new DbException("Open file error");
            }
        }

        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException{
            HeapPageId pid;
            Tuple res;
            if(!this.open)
                throw new NoSuchElementException();
            Tuple t = this.pgItr.next();
            if(t == null){
                try {
                    this.currentPgNo++;
                    pid = new HeapPageId(this.tableId, this.currentPgNo);
                    this.pg = (HeapPage) Database.getBufferPool().getPage(this.tid, pid, this.perm);
                    this.pgItr = this.pg.iterator();
                    if(this.pgItr.hasNext())
                        return this.pgItr.next();
                }catch (IOException e){
                    throw new DbException("Open file error");
                }
            }
            return t;

        }

        public boolean hasNext() throws DbException, TransactionAbortedException{
            HeapPageId pid;
            if(!this.open)
                return false;
            if(this.currentPgNo < this.numPages - 1)
                return true;
            else if(this.currentPgNo == this.numPages - 1){
                return this.pgItr.hasNext();
            }
            else
                return false;

        }

        public void rewind() throws DbException, TransactionAbortedException{
            this.currentPgNo = 0;
        }

        public void close(){
            this.pg = null;
            this.open = false;
            this.currentPgNo = 0;
        }
    }

}

