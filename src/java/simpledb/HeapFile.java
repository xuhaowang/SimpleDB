package simpledb;

import java.io.*;
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
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
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
        //throw new UnsupportedOperationException("implement this");
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
        // not necessary for proj1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        long len = this.f.length();
        return (int)len/BufferPool.PAGE_SIZE;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for proj1
    }



    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new MyIterator(tid);
    }

    private class MyIterator implements DbFileIterator{
        private RandomAccessFile randomAccessFile;
        private int tableId;
        private TransactionId tid;
        private int numPages;
        private int cursor = 0;
        private int currentPgNo = 0;
        private Permissions perm;
        private HeapPage pg;
        public MyIterator(TransactionId tid){
            this.tableId = HeapFile.this.getId();
            this.tid = tid;
            this.numPages = HeapFile.this.numPages();
            this.perm = Permissions.READ_ONLY;
        }
        public void open() throws DbException, TransactionAbortedException{
            HeapPageId pid;
            try {
                this.randomAccessFile = new RandomAccessFile(HeapFile.this.f, "r");
                pid = new HeapPageId(this.tableId, this.currentPgNo);
                this.pg = (HeapPage) Database.getBufferPool().getPage(this.tid, pid, this.perm);
            }catch (IOException e){
                throw new DbException("Open file error");
            }
        }

        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException{
            HeapPageId pid;
            Tuple res;
            if(this.randomAccessFile == null)
                throw new NoSuchElementException();
            if(this.pg == null){
                try{
                    pid = new HeapPageId(this.tableId, this.currentPgNo);
                    this.pg = (HeapPage) Database.getBufferPool().getPage(this.tid, pid, this.perm);
                }catch (IOException e){
                    throw new DbException("Open file error");
                }
            }
            res = this.pg.tuples[this.cursor];
            this.cursor++;
            if(this.cursor == this.pg.getValidNumTuples()){
                this.cursor = 0;
                this.currentPgNo++;
                this.pg = null;
            }
            return res;
        }

        public boolean hasNext() throws DbException, TransactionAbortedException{
            HeapPageId pid;
            if(this.randomAccessFile == null)
                return false;
            if(this.pg == null && this.currentPgNo < this.numPages){
                try{
                    pid = new HeapPageId(this.tableId, this.currentPgNo);
                    this.pg = (HeapPage) Database.getBufferPool().getPage(this.tid, pid, this.perm);
                }catch (IOException e){
                    throw new DbException("Open file error");
                }
            }
            if(this.currentPgNo == this.numPages)
                return false;
            if(this.currentPgNo == this.numPages - 1){
                if(this.cursor >= this.pg.getValidNumTuples() )
                    return false;
            }
            return true;

        }

        public void rewind() throws DbException, TransactionAbortedException{
            this.cursor = 0;
            this.currentPgNo = 0;
        }

        public void close(){
            this.pg = null;
            this.randomAccessFile = null;
            this.cursor = 0;
            this.currentPgNo = 0;
        }
    }

}

