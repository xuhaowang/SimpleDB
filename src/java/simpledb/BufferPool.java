package simpledb;

import java.io.*;
import java.util.ArrayList;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    private Page[] pages = null;
    private Permissions[] perms = null;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.pages = new Page[numPages];
        this.perms = new Permissions[numPages];

    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException, IOException {
        // some code goes here
        LockManager.acquireLock(pid, tid, perm);
        int i = 0;
        int tableid = pid.getTableId();
        while(i < this.pages.length){
            if(this.pages[i] == null || this.pages[i].getId() == null){
                i++;
                continue;
            }
            if(pid.equals(this.pages[i].getId())){
                Page temp = pages[i];
                for(int j = i; j > 0; j--)
                    pages[j] = pages[j - 1];
                pages[0] = temp;
                this.perms[0] = perm;
                return this.pages[0];
            }
            i++;
        }

        HeapFile hf = (HeapFile) Database.getCatalog().getDbFile(tableid);
        HeapPage hp = (HeapPage) hf.readPage(pid);
        i = 0;
        while(i < this.pages.length){
            if(this.pages[i] == null){
                this.pages[i] = hp;
                this.perms[i] = perm;
                break;
            }
            i++;
        }
        if(i == pages.length){
            evictPage();
            //pages[0] = null;
            for(int j = 0; j < pages.length; j++){
                if(pages[j] == null){
                    pages[j] = hp;
                    break;
                }
            }
        }
        LockManager.releaseLock(pid, tid);
        return hp;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here

    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here

        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {

    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock 
     * acquisition is not needed for lab2). May block if the lock cannot 
     * be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        DbFile df = Database.getCatalog().getDbFile(tableId);
        Page res = df.insertTuple(tid, t).get(0);
        boolean in = false;
        boolean isFull = true;
        int flag = 0;
        for(int i = 0; i < pages.length; i++){
            if(pages[i] == null || pages[i].getId() == null){
                isFull = false;
                flag = i;
            }
            else {
                if(res.getId().equals(pages[i].getId())){
                    pages[i].markDirty(true, tid);
                    in = true;
                    break;
                }
            }

        }
        if(!in){
            if(isFull){
                evictPage();
                for(int j = 0; j < pages.length; j++){
                    if(pages[j] == null)
                        pages[j] = res;
                }
            }
            else {
                pages[flag] = res;
                pages[flag].markDirty(false, tid);
            }
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have 
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction adding the tuple.
     * @param t the tuple to add
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
        // some code goes here
        PageId pid = t.getRecordId().getPageId();
        DbFile df = Database.getCatalog().getDbFile(pid.getTableId());
        try {
            HeapPage res = (HeapPage) df.deleteTuple(tid, t);
            if(!res.isEmpty())
                res.markDirty(true, tid);
            else
               res.setPid(null);

        }catch (IOException e){
            throw new DbException("IOException happens");
        }

    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        for(Page p : pages){
            if(p != null && p.getId() != null && p.isDirty() != null){
                PageId pid = p.getId();
                DbFile df = Database.getCatalog().getDbFile(pid.getTableId());
                df.writePage(p);
                p.markDirty(false, null);
            }
        }

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
	// not necessary for proj1
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        for(Page p : pages){
            if(p != null && p.getId() != null && p.getId().equals(pid)){
                DbFile df = Database.getCatalog().getDbFile(pid.getTableId());
                df.writePage(p);
                p.markDirty(false, null);
            }
        }

    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        for(Page p : pages){
            if(p != null && p.getId() != null && p.isDirty() == tid){
                PageId pid = p.getId();
                DbFile df = Database.getCatalog().getDbFile(pid.getTableId());
                df.writePage(p);
                p.markDirty(false, null);
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        int l = pages.length;
        DbFile df = Database.getCatalog().getDbFile(pages[l - 1].getId().getTableId());
        try{
            df.writePage(pages[l - 1]);
        }catch (IOException e){
            throw new DbException("IOException happens");
        }
        for(int i = l - 1; i > 0; i--)
            pages[i] = pages[i - 1];
        pages[0] = null;
    }

}
