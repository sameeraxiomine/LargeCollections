package com.axiomine.largecollections.util;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.Writable;

import com.axiomine.largecollections.serdes.IntegerSerDes;
import com.axiomine.largecollections.serdes.KryoSerDes;
import com.axiomine.largecollections.serdes.TurboDeSerializer;
import com.axiomine.largecollections.serdes.TurboSerializer;
import com.axiomine.largecollections.serdes.WritableSerDes;
import com.google.common.base.Throwables;
import com.google.common.primitives.Ints;

public class WritableList<T extends Writable> extends LargeCollection implements List<Writable>, Serializable {
    public static final long               serialVersionUID = 2l;
    private transient TurboSerializer<Writable> tSerFunc  = new WritableSerDes.SerFunction();    
    private transient TurboDeSerializer<Writable> tDeSerFunc     = null;
    private Class<T> tClass = null;
    
    public WritableList(Class<T> tClass) {
        super();
        this.tClass = tClass;
        this.tDeSerFunc = new WritableSerDes.DeSerFunction(this.tClass);
    }
    
    public WritableList(String dbName,Class<T> tClass) {
        super(dbName);
        this.tClass = tClass;
        this.tDeSerFunc = new WritableSerDes.DeSerFunction(this.tClass);
    }
    
    public WritableList(String dbPath,String dbName,Class<T> tClass) {
        super(dbPath, dbName);
        this.tClass = tClass;
        this.tDeSerFunc = new WritableSerDes.DeSerFunction(this.tClass);
    }
    
    public WritableList(String dbPath,String dbName,int cacheSize,Class<T> tClass) {
        super(dbPath, dbName, cacheSize);
        this.tClass = tClass;
        this.tDeSerFunc = new WritableSerDes.DeSerFunction(this.tClass);
    }
    
    public WritableList(String dbPath,String dbName,int cacheSize,int bloomFilterSize,Class<T> tClass) {
        super(dbPath, dbName, cacheSize, bloomFilterSize);
        this.tClass = tClass;
        this.tDeSerFunc = new WritableSerDes.DeSerFunction(this.tClass);
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
    public Iterator<Writable> iterator() {
        // TODO Auto-generated method stub
        return new MapValueIterator<Writable>(this.getDB(),this.tDeSerFunc);
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean add(Writable e) {
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
        
        Iterator<Writable> iter = this.iterator();
        while(iter.hasNext()){
            Writable e = iter.next();
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
    public boolean addAll(Collection<? extends Writable> c) {
        // TODO Auto-generated method stub
        if(c.size()==0){
            return false;
        }
        else{
            for(Writable e:c){
                this.add(e);
            }
            return true;
        }
    }

    @Override
    public boolean addAll(int index, Collection<? extends Writable> c) {
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
    public Writable get(int index) {
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
    public Writable set(int index, Writable element) {
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
    public void add(int index, Writable element) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Writable remove(int index) {
        if(index<0 || index!=(this.size-1)){
            throw new IndexOutOfBoundsException("Can only remove the last element");
        }
        Writable e = this.get(index);
        db.delete(Ints.toByteArray(index));
        size--;
        return e;
    }

    @Override
    public int indexOf(Object o) {
        int index = -1;
        int myIndex = -1;
        Iterator<Writable> iter = this.iterator();
        while(iter.hasNext()){
            index++;
            Writable e = iter.next();            
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
        Iterator<Writable> iter = this.iterator();
        while(iter.hasNext()){
            index++;
            Writable e = iter.next();            
            if(e.equals(o)){
                myIndex=index;
            }
        }
        return myIndex;

    }

    @Override
    public ListIterator<Writable> listIterator() {
        return new MyListIterator<Writable>(this,this.tSerFunc,this.tDeSerFunc);
    }

    @Override
    public ListIterator<Writable> listIterator(int index) {
        return new MyListIterator<Writable>(this,this.tSerFunc,this.tDeSerFunc,index);
    }

    @Override
    public List<Writable> subList(int fromIndex, int toIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void optimize() {
        try {
            this.initializeBloomFilter();
            MapEntryIterator<Integer, T> iterator = new MapEntryIterator(this, new IntegerSerDes.DeSerFunction(),tDeSerFunc);
            while(iterator.hasNext()){
                Entry<Integer, T> entry = iterator.next();
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
        stream.writeObject(this.tClass);
    }
    
    private void readObject(java.io.ObjectInputStream in) throws IOException,
            ClassNotFoundException {
        this.deserialize(in);
        this.tClass = (Class<T>) in.readObject();
        this.tSerFunc       = new WritableSerDes.SerFunction();
        this.tDeSerFunc     = new WritableSerDes.DeSerFunction(this.tClass);
    }
    /* End of Serialization functions go here */
    
}
