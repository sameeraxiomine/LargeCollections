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

import com.axiomine.largecollections.functions.StringSerDe;
import com.axiomine.largecollections.functions.WritableSerDe;
import org.apache.hadoop.io.*;

public class StringTextMap extends LargeCollection implements   Map<String,Text>, Serializable{
    public static final long               serialVersionUID = 2l;
    private transient Function<String, byte[]> keySerFunc       = new StringSerDe.StringSerFunction();
    private transient Function<Writable, byte[]> valSerFunc  = new WritableSerDe.WritableSerFunction();
    private transient Function<byte[], String> keyDeSerFunc     = new StringSerDe.StringDeSerFunction();
    private transient Function<byte[], Text> valDeSerFunc     = new WritableSerDe.TextDeSerFunction();
    
    public StringTextMap() {
        super();
    }
    
    public StringTextMap(String dbName) {
        super(dbName);
    }
    
    public StringTextMap(String dbPath, String dbName) {
        super(dbPath, dbName);
    }
    
    public StringTextMap(String dbPath, String dbName, int cacheSize) {
        super(dbPath, dbName, cacheSize);
    }
    
    public StringTextMap(String dbPath, String dbName, int cacheSize,
            int bloomFilterSize) {
        super(dbPath, dbName, cacheSize, bloomFilterSize);
    }
    
    @Override
    public void optimize() {
        try {
            this.initializeBloomFilter();
            for (Entry<String, Text> entry : this.entrySet()) {
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
            String ki = (String) key;
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
    public Text get(Object key) {
        byte[] vbytes = null;
        if (key == null) {
            return null;
        }
        String ki = (String) key;
        if (bloomFilter.mightContain((String) key)) {
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
    public Text put(String key, Text value) {
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
    public Text remove(Object key) {
        Text v = null;
        if (key == null)
            return v;
        if (this.size > 0 && this.bloomFilter.mightContain((String) key)) {
            v = this.get(key);
        }
        
        if (v != null) {
            byte[] fullKeyArr = keySerFunc.apply((String) key);
            db.delete(fullKeyArr);
            size--;
        }
        return v;
    }
    
    @Override
    public void putAll(Map<? extends String, ? extends Text> m) {
        try {
            WriteBatch batch = db.createWriteBatch();
            int counter = 0;
            for (Map.Entry<? extends String, ? extends Text> e : m
                    .entrySet()) {
                byte[] keyArr = keySerFunc.apply(e.getKey());
                Text v = null;
                String k = e.getKey();
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
    public Set<String> keySet() {
        return new MapKeySet<String>(this, keyDeSerFunc);
    }
    
    @Override
    public Collection<Text> values() {
        return new ValueCollection<Text>(this, this.getDB(),
                this.valDeSerFunc);
    }
    
    @Override
    public Set<java.util.Map.Entry<String, Text>> entrySet() {
        return new MapEntrySet<String, Text>(this, this.keyDeSerFunc,
                this.valDeSerFunc);
    }
    

    
    /* Serialization functions go here */
    private void writeObject(java.io.ObjectOutputStream stream)
            throws IOException {
        this.serialize(stream);
    }
    
    private void readObject(java.io.ObjectInputStream in) throws IOException,
            ClassNotFoundException {
        keySerFunc       = new StringSerDe.StringSerFunction();
        valSerFunc  = new WritableSerDe.WritableSerFunction();
        keyDeSerFunc     = new StringSerDe.StringDeSerFunction();
        valDeSerFunc     = new WritableSerDe.TextDeSerFunction();
        this.deserialize(in);
    }
    /* End of Serialization functions go here */
    
}
