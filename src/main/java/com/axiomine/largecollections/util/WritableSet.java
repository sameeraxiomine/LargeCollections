package com.axiomine.largecollections.util;

/*
 * Copyright 2014 Sameer Wadkar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.io.Writable;

import com.axiomine.largecollections.serdes.BytesArraySerDes;
import com.axiomine.largecollections.serdes.IntegerSerDes;
import com.axiomine.largecollections.serdes.KryoSerDes;
import com.axiomine.largecollections.serdes.TurboDeSerializer;
import com.axiomine.largecollections.serdes.TurboSerializer;
import com.axiomine.largecollections.serdes.WritableSerDes;
import com.google.common.base.Function;
import com.google.common.base.Throwables;

public class WritableSet<T extends Writable> extends LargeCollection implements Set<Writable>, Serializable {
    private transient TurboSerializer<Writable> tSerFunc  = new WritableSerDes.SerFunction();    
    private transient TurboDeSerializer<Writable> tDeSerFunc     = null;
    private Class<T> tClass = null;
    private byte[] fixedVal = {1};
   
    public WritableSet(Class<T> tClass) {
        super();
        this.tClass = tClass;
        this.tDeSerFunc = new WritableSerDes.DeSerFunction(this.tClass);
    }
    
    public WritableSet(String dbName,Class<T> tClass) {
        super(dbName);
        this.tClass = tClass;
        this.tDeSerFunc = new WritableSerDes.DeSerFunction(this.tClass);
    }
    
    public WritableSet(String dbPath,String dbName,Class<T> tClass) {
        super(dbPath, dbName);
        this.tClass = tClass;
        this.tDeSerFunc = new WritableSerDes.DeSerFunction(this.tClass);
    }
    
    public WritableSet(String dbPath,String dbName,int cacheSize,Class<T> tClass) {
        super(dbPath, dbName, cacheSize);
        this.tClass = tClass;
        this.tDeSerFunc = new WritableSerDes.DeSerFunction(this.tClass);
    }
    
    public WritableSet(String dbPath,String dbName,int cacheSize,int bloomFilterSize,Class<T> tClass) {
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
            if(!this.bloomFilter.mightContain(o))
            {
                return false;
            }
            else{
                byte[] valBytes = null;
                if (o != null) {
                    T t = (T) o;
                    byte[] keyBytes = this.tSerFunc.apply((T)o);
                    valBytes = db.get(keyBytes);
                }
                return valBytes != null;
            }
        }
        return false;
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
        if(this.contains(e)){
            return false;
        }
        else{
            if (e == null)
                return false;
            byte[] fullKeyArr = this.tSerFunc.apply(e);
            bloomFilter.put(e);
            db.put(fullKeyArr, fixedVal);
            size++;
            return true;
        }
    }

    @Override
    public boolean remove(Object o) {
        if (o == null)
            return false;

        if(this.contains(o)){
            byte[] fullKeyArr = this.tSerFunc.apply((T)o);
            db.delete(fullKeyArr);
            size--;
            return true;
        }
        else{
            return false;
        }
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        for(Object o:c){
            if(!this.contains(o)){
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean addAll(Collection<? extends Writable> c) {
        boolean ret = false;
        for(Object o:c){
            boolean rem = this.add((T)o);
            ret = rem || ret;
        }
        return ret;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        boolean retVal = false;
        Iterator<Writable> iter = this.iterator();
        while(iter.hasNext()){
            Writable t = iter.next();
            if(!c.contains(t)){
                this.remove(t);
                retVal=true;
            }
        }
        try{
            ((Closeable) iter).close();    
        }
        catch(Exception ex){
            throw Throwables.propagate(ex);
        }
        
        return retVal;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        boolean ret = false;
        for(Object o:c){
            boolean rem = this.remove(o);
            ret = rem || ret;
        }
        return ret;
    }

    @Override
    public void clear() {
        this.clearDB();
        
    }

    @Override
    public void optimize() {
        try {
            this.initializeBloomFilter();
            MapEntryIterator<Writable, byte[]> iterator = new MapEntryIterator<Writable, byte[]>(this, tDeSerFunc,new BytesArraySerDes.DeSerFunction());
            while(iterator.hasNext()){
                Entry<Writable, byte[]> entry = iterator.next();
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

    @Override
    public Iterator<Writable> iterator() {
        // TODO Auto-generated method stub
        return new MapKeyIterator<Writable>(this,this.tDeSerFunc);
    }

}