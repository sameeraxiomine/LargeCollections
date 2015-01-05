package com.axiomine.largecollections.turboutil;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map.Entry;

import com.axiomine.largecollections.serdes.*;
import com.google.common.base.Throwables;
import com.google.common.primitives.Ints;
import com.axiomine.largecollections.util.*;

import org.apache.hadoop.io.*;

public class BooleanWritableList extends LargeCollection implements List<BooleanWritable>, Serializable {
    public static final long               serialVersionUID = 2l;
    private transient TurboSerializer<BooleanWritable> tSerFunc       =  new WritableSerDes.BooleanWritableSerFunction();    
    private transient TurboDeSerializer<BooleanWritable> tDeSerFunc  = new WritableSerDes.BooleanWritableDeSerFunction();    
    

    
    public BooleanWritableList() {
        super();
    }
    
    public BooleanWritableList(String dbName) {
        super(dbName);
    }
    
    public BooleanWritableList(String dbPath,String dbName) {
        super(dbPath, dbName);
    }
    
    public BooleanWritableList(String dbPath,String dbName,int cacheSize) {
        super(dbPath, dbName, cacheSize);
    }
    
    public BooleanWritableList(String dbPath,String dbName,int cacheSize,int bloomFilterSize) {
        super(dbPath, dbName, cacheSize, bloomFilterSize);
    }


    @Override
    public int size() {
        return this.size;
    }

    @Override
    public boolean isEmpty() {
        return (this.size==0);
    }

    @Override
    public boolean contains(Object o) {
        if (o != null) {
            return this.bloomFilter.mightContain(o);        
        }
        return false;
    }

    @Override
    public Iterator<BooleanWritable> iterator() {
        // TODO Auto-generated method stub
        return new MapValueIterator<BooleanWritable>(this.getDB(),this.tDeSerFunc);
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <BooleanWritable>  BooleanWritable[] toArray(BooleanWritable[] a) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean add(BooleanWritable e) {
        if (e == null)
            return false;
        else{
            byte[] fullKeyArr = Ints.toByteArray(size);
            byte[] fullValArr = this.tSerFunc.apply(e);
            db.put(fullKeyArr, fullValArr);
            this.bloomFilter.put(e);
            this.size++;
        }
        return true;
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        if(c.size()==0){//Optimization. Do not traverse the whole list for empty collection
            return true;
        }
        boolean mightContain=true;
        for(Object e:c){
            mightContain = this.bloomFilter.mightContain(c);
            if(!mightContain){
                break;
            }
        }
        if(!mightContain){
            return false;
        }
        
        Iterator<BooleanWritable> iter = this.iterator();
        while(iter.hasNext()){
            BooleanWritable e = iter.next();
            if(c.contains(e)){
                while(true){
                    boolean removed = c.remove(e);
                    if(!removed)
                        break;//All instances removed
                }
            }
            else{
                break;
            }
        }
        return (c.size()==0);
    }

    @Override
    public boolean addAll(Collection<? extends BooleanWritable> c) {
        // TODO Auto-generated method stub
        if(c.size()==0){
            return false;
        }
        else{
            for(BooleanWritable e:c){
                this.add(e);
            }
            return true;
        }
    }

    @Override
    public boolean addAll(int index, Collection<? extends BooleanWritable> c) {
        throw new UnsupportedOperationException("can only use addAll(Collection<? extends T> c)");
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        this.clearDB();
    }

    @Override
    public BooleanWritable get(int index) {
        if(index<0 || index>=this.size()){
            throw new IndexOutOfBoundsException();
        }
        byte[] vbytes = db.get(Ints.toByteArray(index));
        if (vbytes == null) {
            return null;
        } else {
            return this.tDeSerFunc.apply(vbytes);
        }
    }

    @Override
    public BooleanWritable set(int index, BooleanWritable element) {
        if(index<0 || index>=this.size()){
            throw new IndexOutOfBoundsException();
        }
        byte[] fullKeyArr = Ints.toByteArray(index);
        byte[] fullValArr = this.tSerFunc.apply(element);
        db.put(fullKeyArr, fullValArr);
        this.bloomFilter.put(element);
        return element;
    }

    @Override
    public void add(int index, BooleanWritable element) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BooleanWritable remove(int index) {
        if(index<0 || index!=(this.size-1)){
            throw new IndexOutOfBoundsException("Can only remove the last element");
        }
        BooleanWritable e = this.get(index);
        db.delete(Ints.toByteArray(index));
        size--;
        return e;
    }

    @Override
    public int indexOf(Object o) {
        int index = -1;
        int myIndex = -1;
        Iterator<BooleanWritable> iter = this.iterator();
        while(iter.hasNext()){
            index++;
            BooleanWritable e = iter.next();            
            if(e.equals(o)){
                myIndex=index;
                break;
            }
        }
        return myIndex;
    }

    @Override
    public int lastIndexOf(Object o) {
        int index = -1;
        int myIndex = -1;
        Iterator<BooleanWritable> iter = this.iterator();
        while(iter.hasNext()){
            index++;
            BooleanWritable e = iter.next();            
            if(e.equals(o)){
                myIndex=index;
            }
        }
        return myIndex;

    }

    @Override
    public ListIterator<BooleanWritable> listIterator() {
        return new MyListIterator<BooleanWritable>(this,this.tSerFunc,this.tDeSerFunc);
    }

    @Override
    public ListIterator<BooleanWritable> listIterator(int index) {
        return new MyListIterator<BooleanWritable>(this,this.tSerFunc,this.tDeSerFunc,index);
    }

    @Override
    public List<BooleanWritable> subList(int fromIndex, int toIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void optimize() {
        try {
            this.initializeBloomFilter();
            MapEntryIterator<Integer, BooleanWritable> iterator = new MapEntryIterator(this, new WritableSerDes.BooleanWritableDeSerFunction(),tDeSerFunc);
            while(iterator.hasNext()){
                Entry<Integer, BooleanWritable> entry = iterator.next();
                this.bloomFilter.put(entry.getKey());
            }
        } catch (Exception ex) {
            throw Throwables.propagate(ex);
        }
        
    }
    
    
    /* Serialization functions go here */
    private void writeObject(java.io.ObjectOutputStream stream)
            throws IOException {
        this.serialize(stream);
    }
    
    private void readObject(java.io.ObjectInputStream in) throws IOException,
            ClassNotFoundException {
        this.deserialize(in);
        tSerFunc       =  new WritableSerDes.BooleanWritableSerFunction();    
        tDeSerFunc  = new WritableSerDes.BooleanWritableDeSerFunction();    
    }
    /* End of Serialization functions go here */
    
}