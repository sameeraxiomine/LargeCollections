/*
 * Copyright 2014 Axiomine
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
package com.axiomine.largecollections.turboutil;
import com.google.common.base.Throwables;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.iq80.leveldb.WriteBatch;

import com.google.common.base.Function;


import com.axiomine.largecollections.util.*;
import com.axiomine.largecollections.serdes.*;
import com.axiomine.largecollections.kryo.serializers.*;


import org.apache.hadoop.io.*;

public class ShortWritableIntegerMap extends LargeCollection implements   Map<ShortWritable,Integer>, Serializable{
    public static final long               serialVersionUID = 2l;
    
    private transient TurboSerializer<ShortWritable> keySerFunc  = new WritableSerDes.ShortWritableSerFunction();
    private transient TurboSerializer<Integer> valSerFunc       = new IntegerSerDes.SerFunction();    
    private transient TurboDeSerializer<ShortWritable> keyDeSerFunc     = new WritableSerDes.ShortWritableDeSerFunction();
    private transient TurboDeSerializer< Integer> valDeSerFunc     = new IntegerSerDes.DeSerFunction();
    
    public ShortWritableIntegerMap() {
        super();
    }
    
    public ShortWritableIntegerMap(String dbName) {
        super(dbName);
    }
    
    public ShortWritableIntegerMap(String dbPath, String dbName) {
        super(dbPath, dbName);
    }
    
    public ShortWritableIntegerMap(String dbPath, String dbName, int cacheSize) {
        super(dbPath, dbName, cacheSize);
    }
    
    public ShortWritableIntegerMap(String dbPath, String dbName, int cacheSize,
            int bloomFilterSize) {
        super(dbPath, dbName, cacheSize, bloomFilterSize);
    }
    
    @Override
    public void optimize() {
        MapKeySet<ShortWritable> keys = new MapKeySet<ShortWritable>(this, keyDeSerFunc);
        try {
            this.initializeBloomFilter();
            for (ShortWritable entry : keys) {
                this.bloomFilter.put(entry);
            }
        } catch (Exception ex) {
            throw Throwables.propagate(ex);
        }
        finally{
            if(keys!=null){
                try{
                    keys.close();
                }
                catch(Exception ex){
                    throw Throwables.propagate(ex);
                }                
            }
        }
    }
    
    @Override
    public boolean containsKey(Object key) {
        byte[] valBytes = null;
        if (key != null) {
            ShortWritable ki = (ShortWritable) key;
            if (this.bloomFilter.mightContain(ki)) {
                byte[] keyBytes = keySerFunc.apply(ki);
                valBytes = db.get(keyBytes);
            }
        }
        return valBytes != null;
    }
    
    @Override
    public boolean containsValue(Object value) {
        // Will be very slow unless a seperate DB is maintained with values as
        // keys
        throw new UnsupportedOperationException();
        
    }
    
    @Override
    public Integer get(Object key) {
        byte[] vbytes = null;
        if (key == null) {
            return null;
        }
        ShortWritable ki = (ShortWritable) key;
        if (bloomFilter.mightContain((ShortWritable) key)) {
            vbytes = db.get(keySerFunc.apply(ki));
            if (vbytes == null) {
                return null;
            } else {
                return valDeSerFunc.apply(vbytes);
            }
        } else {
            return null;
        }
        
    }
    
    @Override
    public int size() {
        return (int) size;
    }
    
    @Override
    public boolean isEmpty() {
        return size == 0;
    }
    
    /* Putting null values is not allowed for this map */
    @Override
    public Integer put(ShortWritable key, Integer value) {
        if (key == null)
            return null;
        if (value == null)// Do not add null key or value
            return null;
        byte[] fullKeyArr = keySerFunc.apply(key);
        byte[] fullValArr = valSerFunc.apply(value);
        if (!this.containsKey(key)) {
            bloomFilter.put(key);
            db.put(fullKeyArr, fullValArr);
            size++;
        } else {
            db.put(fullKeyArr, fullValArr);
        }
        return value;
    }
    
    @Override
    public Integer remove(Object key) {
        Integer v = null;
        if (key == null)
            return v;
        if (this.size > 0 && this.bloomFilter.mightContain((ShortWritable) key)) {
            v = this.get(key);
        }
        
        if (v != null) {
            byte[] fullKeyArr = keySerFunc.apply((ShortWritable) key);
            db.delete(fullKeyArr);
            size--;
        }
        return v;
    }
    
    @Override
    public void putAll(Map<? extends ShortWritable, ? extends Integer> m) {
        try {
            WriteBatch batch = db.createWriteBatch();
            int counter = 0;
            for (Map.Entry<? extends ShortWritable, ? extends Integer> e : m
                    .entrySet()) {
                byte[] keyArr = keySerFunc.apply(e.getKey());
                Integer v = null;
                ShortWritable k = e.getKey();
                if (this.size > 0 && this.bloomFilter.mightContain(k)) {
                    v = this.get(k);
                }
                if (v == null) {
                    bloomFilter.put(k);
                    this.size++;
                }
                batch.put(keyArr, valSerFunc.apply(e.getValue()));
                counter++;
                if (counter % 1000 == 0) {
                    db.write(batch);
                    batch.close();
                    batch = db.createWriteBatch();
                }
            }
            db.write(batch);
            batch.close();
        } catch (Exception ex) {
            throw Throwables.propagate(ex);
        }
        
    }
    
    @Override
    public void clear() {
        this.clearDB();
    }
    
    /* Iterators and Collections based on this Map */
    @Override
    public Set<ShortWritable> keySet() {
        return new MapKeySet<ShortWritable>(this, keyDeSerFunc);
    }
    
    @Override
    public Collection<Integer> values() {
        return new ValueCollection<Integer>(this, this.getDB(),
                this.valDeSerFunc);
    }
    
    @Override
    public Set<java.util.Map.Entry<ShortWritable, Integer>> entrySet() {
        return new MapEntrySet<ShortWritable, Integer>(this, this.keyDeSerFunc,
                this.valDeSerFunc);
    }
    

    
    /* Serialization functions go here */
    private void writeObject(java.io.ObjectOutputStream stream)
            throws IOException {
        this.serialize(stream);
    }
    
    private void readObject(java.io.ObjectInputStream in) throws IOException,
            ClassNotFoundException {
        keySerFunc  = new WritableSerDes.ShortWritableSerFunction();
        valSerFunc       = new IntegerSerDes.SerFunction();    
        keyDeSerFunc     = new WritableSerDes.ShortWritableDeSerFunction();
        valDeSerFunc     = new IntegerSerDes.DeSerFunction();
        this.deserialize(in);
    }
    /* End of Serialization functions go here */
    
}
