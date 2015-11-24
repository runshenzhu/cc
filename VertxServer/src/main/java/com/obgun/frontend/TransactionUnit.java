package com.obgun.frontend;

/**
 * Created by zrsh on 11/23/15.
 */
public class TransactionUnit {
    class Slot{
        Boolean release;
        final Object lock = new Object();
        String tag;
        long tweetId;
        public Slot(){
            release = false;
            tweetId = 0;
        }
    }
    Slot[] slots;
    long transId;
    public TransactionUnit(long transId){
        slots = new Slot[5];
        for(int i = 0; i < 5; i++){
            slots[i] = new Slot();
        }
        this.transId = transId;
    }

    final public void write(String sId, String tweetId, String tag){
        int slotId = Integer.parseInt(sId) - 1;
        long tid = Long.parseLong(tweetId);
        Slot slot = slots[slotId];
        slot.tag = tag;
        slot.tweetId = tid;
        synchronized (slot.lock){
            slot.release = true;
            slot.lock.notifyAll();
        }
    }

    final public String read(String sId, String tweetId){
        int slotId = Integer.parseInt(sId) - 1;
        long tid = Long.parseLong(tweetId);
        Slot slot = slots[slotId];
        slot.tweetId = -1; // mark tweetId as -1, so nobody will match it
        String text = SQLHandler.getSqlAnswerInternalQ6(tid);
        synchronized (slot.lock){
            slot.release = true;
            slot.lock.notifyAll();
        }

        for(int s = slotId - 1; s >= 0; s--){
            slot = slots[s];
            synchronized (slot.lock){
                while(!slot.release){
                    try {
                        slot.lock.wait();
                    }catch (InterruptedException ignore){}
                }
            }
            if(tid == slot.tweetId){
                return text + slot.tag;
            }
        }
        return text;
    }

    final public void endChecker(){
        for(int s = 4; s >= 0; s--) {
            Slot slot = slots[s];
            synchronized (slot.lock) {
                while (!slot.release) {
                    try {
                        slot.lock.wait();
                    } catch (InterruptedException ignore) {}
                }
            }
        }
    }
}
