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
package com.axiomine.largecollections;
import com.google.common.base.Throwables;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.iq80.leveldb.WriteBatch;

import com.google.common.base.Function;


import com.axiomine.largecollections.*;
import com.axiomine.largecollections.functions.FloatSerDe;
import com.axiomine.largecollections.functions.StringSerDe;


public class FloatStringMap extends LargeCollection implements   Map<Float,String>, Serializable{
    public static final long               serialVersionUID = 2l;
    private transient Function<Float, byte[]> keySerFunc       = new FloatSerDe.FloatSerFunction();
    private transient Function<String, byte[]> valSerFunc       = new StringSerDe.StringSerFunction();
    private transient Function<byte[], Float> keyDeSerFunc     = new FloatSerDe.FloatDeSerFunction();
    private transient Function<byte[], String> valDeSerFunc     = new StringSerDe.StringDeSerFunction();
    
    public FloatStringMap() {
        super();
    }
    
    public FloatStringMap(String dbName) {
        super(dbName);
    }
    
    public FloatStringMap(String dbPath, String dbName) {
        super(dbPath, dbName);
    }
    
    public FloatStringMap(String dbPath, String dbName, int cacheSize) {
        super(dbPath, dbName, cacheSize);
    }
    
    public FloatStringMap(String dbPath, String dbName, int cacheSize,
            int bloomFilterSize) {
        super(dbPath, dbName, cacheSize, bloomFilterSize);
    }
    
    @Override
    public void optimize() {
        try {
            this.initializeBloomFilter();
            for (Entry<Float, String> entry : this.entrySet()) {
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
            Float ki = (Float) key;
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
    public String get(Object key) {
        byte[] vbytes = null;
        if (key == null) {
            return null;
        }
        Float ki = (Float) key;
        if (bloomFilter.mightContain((Float) key)) {
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
    public String put(Float key, String value) {
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
    public String remove(Object key) {
        String v = null;
        if (key == null)
            return v;
        if (this.size > 0 && this.bloomFilter.mightContain((Float) key)) {
            v = this.get(key);
        }
        
        if (v != null) {
            byte[] fullKeyArr = keySerFunc.apply((Float) key);
            db.delete(fullKeyArr);
            size--;
        }
        return v;
    }
    
    @Override
    public void putAll(Map<? extends Float, ? extends String> m) {
        try {
            WriteBatch batch = db.createWriteBatch();
            int counter = 0;
            for (Map.Entry<? extends Float, ? extends String> e : m
                    .entrySet()) {
                byte[] keyArr = keySerFunc.apply(e.getKey());
                String v = null;
                Float k = e.getKey();
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
        this.initialize();
        this.initializeBloomFilter();
    }
    
    /* Iterators and Collections based on this Map */
    @Override
    public Set<Float> keySet() {
        return new MapKeySet<Float>(this, keyDeSerFunc);
    }
    
    @Override
    public Collection<String> values() {
        return new ValueCollection<String>(this, this.getDB(),
                this.valDeSerFunc);
    }
    
    @Override
    public Set<java.util.Map.Entry<Float, String>> entrySet() {
        return new MapEntrySet<Float, String>(this, this.keyDeSerFunc,
                this.valDeSerFunc);
    }
    

    
    /* Serialization functions go here */
    private void writeObject(java.io.ObjectOutputStream stream)
            throws IOException {
        this.serialize(stream);
    }
    
    private void readObject(java.io.ObjectInputStream in) throws IOException,
            ClassNotFoundException {
        keySerFunc = new FloatSerDe.FloatSerFunction();
        valSerFunc = new StringSerDe.StringSerFunction();
        keyDeSerFunc = new FloatSerDe.FloatDeSerFunction();
        valDeSerFunc = new StringSerDe.StringDeSerFunction();
        this.deserialize(in);
    }
    /* End of Serialization functions go here */
    
}
