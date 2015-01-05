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

public class ByteWritableList extends LargeCollection implements List<ByteWritable>, Serializable {
    public static final long               serialVersionUID = 2l;
    private transient TurboSerializer<ByteWritable> tSerFunc       =  new WritableSerDes.ByteWritableSerFunction();    
    private transient TurboDeSerializer<ByteWritable> tDeSerFunc  = new WritableSerDes.ByteWritableDeSerFunction();    
    

    
    public ByteWritableList() {
        super();
    }
    
    public ByteWritableList(String dbName) {
        super(dbName);
    }
    
    public ByteWritableList(String dbPath,String dbName) {
        super(dbPath, dbName);
    }
    
    public ByteWritableList(String dbPath,String dbName,int cacheSize) {
        super(dbPath, dbName, cacheSize);
    }
    
    public ByteWritableList(String dbPath,String dbName,int cacheSize,int bloomFilterSize) {
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
    public Iterator<ByteWritable> iterator() {
        // TODO Auto-generated method stub
        return new MapValueIterator<ByteWritable>(this.getDB(),this.tDeSerFunc);
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <ByteWritable>  ByteWritable[] toArray(ByteWritable[] a) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean add(ByteWritable e) {
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
        
        Iterator<ByteWritable> iter = this.iterator();
        while(iter.hasNext()){
            ByteWritable e = iter.next();
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
    public boolean addAll(Collection<? extends ByteWritable> c) {
        // TODO Auto-generated method stub
        if(c.size()==0){
            return false;
        }
        else{
            for(ByteWritable e:c){
                this.add(e);
            }
            return true;
        }
    }

    @Override
    public boolean addAll(int index, Collection<? extends ByteWritable> c) {
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
    public ByteWritable get(int index) {
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
    public ByteWritable set(int index, ByteWritable element) {
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
    public void add(int index, ByteWritable element) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteWritable remove(int index) {
        if(index<0 || index!=(this.size-1)){
            throw new IndexOutOfBoundsException("Can only remove the last element");
        }
        ByteWritable e = this.get(index);
        db.delete(Ints.toByteArray(index));
        size--;
        return e;
    }

    @Override
    public int indexOf(Object o) {
        int index = -1;
        int myIndex = -1;
        Iterator<ByteWritable> iter = this.iterator();
        while(iter.hasNext()){
            index++;
            ByteWritable e = iter.next();            
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
        Iterator<ByteWritable> iter = this.iterator();
        while(iter.hasNext()){
            index++;
            ByteWritable e = iter.next();            
            if(e.equals(o)){
                myIndex=index;
            }
        }
        return myIndex;

    }

    @Override
    public ListIterator<ByteWritable> listIterator() {
        return new MyListIterator<ByteWritable>(this,this.tSerFunc,this.tDeSerFunc);
    }

    @Override
    public ListIterator<ByteWritable> listIterator(int index) {
        return new MyListIterator<ByteWritable>(this,this.tSerFunc,this.tDeSerFunc,index);
    }

    @Override
    public List<ByteWritable> subList(int fromIndex, int toIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void optimize() {
        try {
            this.initializeBloomFilter();
            MapEntryIterator<Integer, ByteWritable> iterator = new MapEntryIterator(this, new WritableSerDes.ByteWritableDeSerFunction(),tDeSerFunc);
            while(iterator.hasNext()){
                Entry<Integer, ByteWritable> entry = iterator.next();
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
        tSerFunc       =  new WritableSerDes.ByteWritableSerFunction();    
        tDeSerFunc  = new WritableSerDes.ByteWritableDeSerFunction();    
    }
    /* End of Serialization functions go here */
    
}