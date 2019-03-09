package simpledb;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
    public static final int SHAERD_LOCK = 0;
    public static final int EXCLUSIVE_LOCK = 1;
    public static ConcurrentHashMap<PageId, ConcurrentHashMap<TransactionId, Lock>> lockMap = new ConcurrentHashMap<>();

    /**
     * get the shared lock if there are no other transaction have exclusive lock on this page;
     * otherwise, the process will be blocked
     * @param pid the page id
     * @param tid the transaction id
     */
    public static void acquireShareLock(PageId pid, TransactionId tid){
       if(!lockMap.containsKey(pid)){
           ConcurrentHashMap<TransactionId, Lock> tranMap = new ConcurrentHashMap<>();
           ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
           AtomicInteger atomInt = new AtomicInteger(SHAERD_LOCK);
           Lock lock = new Lock(rwl, atomInt);
           rwl.readLock().lock();
           tranMap.put(tid, lock);
           lockMap.put(pid, tranMap);
           System.out.println(tranMap.size());
       }
       else {
           ConcurrentHashMap<TransactionId, Lock> tranMap = lockMap.get(pid);
           for(Map.Entry<TransactionId, Lock> e : tranMap.entrySet()){
               if(e.getValue().atomInt.get() == EXCLUSIVE_LOCK){
                   if(e.getKey().equals(tid))
                       return;
                   while ( tranMap.size() != 0);
               }
           }
           ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
           AtomicInteger atomInt = new AtomicInteger(SHAERD_LOCK);
           Lock lock = new Lock(rwl, atomInt);
           rwl.readLock().lock();
           tranMap.put(tid, lock);
       }
    }


    /**
     * get the exclusive lock if there are no other transaction have lock on this page;
     * otherwise, the process will be blocked
     * @param pid the page id
     * @param tid the transaction id
     */
    public static void acquireExclusiveLock(PageId pid, TransactionId tid){
        if(!lockMap.containsKey(pid)){
            ConcurrentHashMap<TransactionId, Lock> tranMap = new ConcurrentHashMap<>();
            ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
            AtomicInteger atomInt = new AtomicInteger(EXCLUSIVE_LOCK);
            Lock lock = new Lock(rwl, atomInt);
            rwl.readLock().lock();
            tranMap.put(tid, lock);
            lockMap.put(pid, tranMap);
        }
        else {
            ConcurrentHashMap<TransactionId, Lock> tranMap = lockMap.get(pid);
            while (tranMap.size() > 1);
            System.out.println(tranMap.size());
            for(Map.Entry<TransactionId, Lock> e : tranMap.entrySet()){
                if(e.getKey() == tid){
                    if(e.getValue().atomInt.get() == SHAERD_LOCK){
                        e.getValue().atomInt.compareAndSet(SHAERD_LOCK, EXCLUSIVE_LOCK);
                        return;
                    }
                    else return;
                }
                else {
                    while (tranMap.size() != 0);
                    ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
                    AtomicInteger atomInt = new AtomicInteger(EXCLUSIVE_LOCK);
                    Lock lock = new Lock(rwl, atomInt);
                    rwl.readLock().lock();
                    tranMap.put(tid, lock);
                    return;
                }
            }
        }
    }

    public static void acquireLock(PageId pid, TransactionId tid, Permissions perm){
        if(perm == Permissions.READ_ONLY)
            acquireShareLock(pid, tid);
        else{
            acquireExclusiveLock(pid, tid);
        }
    }

    /**
     * Release lock hold by the transaction on the page
     * @param pid the page id
     * @param tid the transaction id
     * @throws DbException
     */

    public static void releaseLock(PageId pid, TransactionId tid) throws DbException{

        if(!lockMap.containsKey(pid))
            throw new DbException("no lock on this page");
        ConcurrentHashMap<TransactionId, Lock> tranMap = lockMap.get(pid);
        if(!tranMap.containsKey(tid))
            throw new DbException("this transaction have no lock on the page");
        Lock l = tranMap.get(tid);
        l.rwl.readLock().unlock();
        tranMap.remove(tid);
    }

    static class Lock {
       public ReentrantReadWriteLock rwl;
       public AtomicInteger atomInt;
       Lock(ReentrantReadWriteLock rwl, AtomicInteger atomInt){
           this.rwl = rwl;
           this.atomInt = atomInt;
       }
    }
}
