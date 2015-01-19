package com.axiomine.largecollections.turboutil;


/*
 * Copyright 2015 Axomine LLC
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

import org.iq80.leveldb.DBIterator;

import com.axiomine.largecollections.util.*;
import com.axiomine.largecollections.serdes.*;
import com.google.common.base.Function;
import com.google.common.base.Throwables;

public class BytesArraySet extends LargeCollection implements Set<byte[]>, Serializable {
    private transient TurboSerializer<byte[]> tSerFunc       =  new BytesArraySerDes.SerFunction();
    private transient TurboDeSerializer<byte[]> tDeSerFunc       = new BytesArraySerDes.DeSerFunction();
   
    public BytesArraySet() {
        super();
    }
    
    public BytesArraySet(String dbName) {
        super(dbName);
    }
    
    public BytesArraySet(String dbPath,String dbName) {
        super(dbPath, dbName);
    }
    
    public BytesArraySet(String dbPath,String dbName,int cacheSize) {
        super(dbPath, dbName, cacheSize);
    }
    
    public BytesArraySet(String dbPath,String dbName,int cacheSize,int bloomFilterSize) {
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
            if(!this.bloomFilter.mightContain(o))
            {
                return false;
            }
            else{
                byte[] valBytes = null;
                if (o != null) {
                    byte[] t = (byte[]) o;
                    byte[] keyBytes = this.tSerFunc.apply((byte[])o);
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
    public <T>  T[] toArray(T[] a) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public boolean add(byte[] e) {
        if(this.contains(e)){
            return false;
        }
        else{
            if (e == null)
                return false;
            byte[] arr = this.tSerFunc.apply(e);
            bloomFilter.put(e);
            db.put(arr, TurboSet.fixedVal);
            size++;
            return true;
        }
    }

    @Override
    public boolean remove(Object o) {
        if (o == null)
            return false;

        if(this.contains(o)){
            byte[] fullKeyArr = this.tSerFunc.apply((byte[])o);
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
    public boolean addAll(Collection<? extends byte[]> c) {
        boolean ret = false;
        for(Object o:c){
            boolean rem = this.add((byte[])o);
            ret = rem || ret;
        }
        return ret;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        boolean retVal = false;
        Iterator<byte[]> iter = this.iterator();
        while(iter.hasNext()){
            byte[] t = iter.next();
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
        DBIterator iterator = this.getDB().iterator();
        try {
            this.initializeBloomFilter();
            while (iterator.hasNext()) {
                this.bloomFilter.put(tDeSerFunc.apply(iterator.next()
                        .getKey()));
            }
        } catch (Exception ex) {
            throw Throwables.propagate(ex);
        } finally {
            try {
                iterator.close();
            } catch (Exception ex) {
                throw Throwables.propagate(ex);
            }
            
        }

    }
    
    /* Serialization functions go here */
    private void writeObject(java.io.ObjectOutputStream stream)
            throws IOException {
        this.serialize(stream);
        stream.writeObject(this.tSerFunc);
        stream.writeObject(this.tDeSerFunc);
    }
    
    private void readObject(java.io.ObjectInputStream in) throws IOException,
            ClassNotFoundException {
        this.deserialize(in);
        this.tSerFunc         = (TurboSerializer<byte[]>)in.readObject();
        this.tDeSerFunc       = (TurboDeSerializer<byte[]>)in.readObject();
    }

    @Override
    public Iterator<byte[]> iterator() {
        // TODO Auto-generated method stub
        return new MapKeyIterator<byte[]>(this,this.tDeSerFunc);
    }

}