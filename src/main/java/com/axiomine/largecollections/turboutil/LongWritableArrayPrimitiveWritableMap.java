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


import com.axiomine.largecollections.serdes.basic.*;
import com.axiomine.largecollections.util.*;

import org.apache.hadoop.io.*;

public class LongWritableArrayPrimitiveWritableMap extends LargeCollection implements   Map<LongWritable,ArrayPrimitiveWritable>, Serializable{
    public static final long               serialVersionUID = 2l;
    
    private transient Function<Writable, byte[]> keySerFunc  = new WritableSerDe.SerFunction();
    private transient Function<Writable, byte[]> valSerFunc  = new WritableSerDe.SerFunction();    
    private transient Function<byte[], LongWritable> keyDeSerFunc     = new WritableSerDe.LongWritableDeSerFunction();
    private transient Function<byte[], ArrayPrimitiveWritable> valDeSerFunc     = new WritableSerDe.ArrayPrimitiveWritableDeSerFunction();
    
    public LongWritableArrayPrimitiveWritableMap() {
        super();
    }
    
    public LongWritableArrayPrimitiveWritableMap(String dbName) {
        super(dbName);
    }
    
    public LongWritableArrayPrimitiveWritableMap(String dbPath, String dbName) {
        super(dbPath, dbName);
    }
    
    public LongWritableArrayPrimitiveWritableMap(String dbPath, String dbName, int cacheSize) {
        super(dbPath, dbName, cacheSize);
    }
    
    public LongWritableArrayPrimitiveWritableMap(String dbPath, String dbName, int cacheSize,
            int bloomFilterSize) {
        super(dbPath, dbName, cacheSize, bloomFilterSize);
    }
    
    @Override
    public void optimize() {
        try {
            this.initializeBloomFilter();
            for (Entry<LongWritable, ArrayPrimitiveWritable> entry : this.entrySet()) {
                this.bloomFilter.put(entry.getKey());
            }
        } catch (Exception ex) {
            throw Throwables.propagate(ex);
        }
    }
    
    @Override
    public boolean containsKey(Object key) {
        byte[] valBytes = null;
        if (key != null) {
            LongWritable ki = (LongWritable) key;
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
    public ArrayPrimitiveWritable get(Object key) {
        byte[] vbytes = null;
        if (key == null) {
            return null;
        }
        LongWritable ki = (LongWritable) key;
        if (bloomFilter.mightContain((LongWritable) key)) {
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
    public ArrayPrimitiveWritable put(LongWritable key, ArrayPrimitiveWritable value) {
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
    public ArrayPrimitiveWritable remove(Object key) {
        ArrayPrimitiveWritable v = null;
        if (key == null)
            return v;
        if (this.size > 0 && this.bloomFilter.mightContain((LongWritable) key)) {
            v = this.get(key);
        }
        
        if (v != null) {
            byte[] fullKeyArr = keySerFunc.apply((LongWritable) key);
            db.delete(fullKeyArr);
            size--;
        }
        return v;
    }
    
    @Override
    public void putAll(Map<? extends LongWritable, ? extends ArrayPrimitiveWritable> m) {
        try {
            WriteBatch batch = db.createWriteBatch();
            int counter = 0;
            for (Map.Entry<? extends LongWritable, ? extends ArrayPrimitiveWritable> e : m
                    .entrySet()) {
                byte[] keyArr = keySerFunc.apply(e.getKey());
                ArrayPrimitiveWritable v = null;
                LongWritable k = e.getKey();
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
    public Set<LongWritable> keySet() {
        return new MapKeySet<LongWritable>(this, keyDeSerFunc);
    }
    
    @Override
    public Collection<ArrayPrimitiveWritable> values() {
        return new ValueCollection<ArrayPrimitiveWritable>(this, this.getDB(),
                this.valDeSerFunc);
    }
    
    @Override
    public Set<java.util.Map.Entry<LongWritable, ArrayPrimitiveWritable>> entrySet() {
        return new MapEntrySet<LongWritable, ArrayPrimitiveWritable>(this, this.keyDeSerFunc,
                this.valDeSerFunc);
    }
    

    
    /* Serialization functions go here */
    private void writeObject(java.io.ObjectOutputStream stream)
            throws IOException {
        this.serialize(stream);
    }
    
    private void readObject(java.io.ObjectInputStream in) throws IOException,
            ClassNotFoundException {
        keySerFunc  = new WritableSerDe.SerFunction();
        valSerFunc  = new WritableSerDe.SerFunction();    
        keyDeSerFunc     = new WritableSerDe.LongWritableDeSerFunction();
        valDeSerFunc     = new WritableSerDe.ArrayPrimitiveWritableDeSerFunction();
        this.deserialize(in);
    }
    /* End of Serialization functions go here */
    
}
