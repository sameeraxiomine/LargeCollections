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
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.iq80.leveldb.WriteBatch;

import com.axiomine.largecollections.functions.IntegerSerDe;
import com.google.common.base.Function;
import com.google.common.base.Throwables;


public class MyTestMap extends LargeCollection implements   Map<Integer,Integer>, Serializable{
    public static final long               serialVersionUID = 2l;
    private transient Function<Integer, byte[]> keySerFunc       = new IntegerSerDe.IntegerSerFunction();
    private transient Function<Integer, byte[]> valSerFunc       = new IntegerSerDe.IntegerSerFunction();
    private transient Function<byte[], Integer> keyDeSerFunc     = new IntegerSerDe.IntegerDeSerFunction();
    private transient Function<byte[], Integer> valDeSerFunc     = new IntegerSerDe.IntegerDeSerFunction();
    
    public MyTestMap() {
        super();
    }
    
    public MyTestMap(String dbName) {
        super(dbName);
    }
    
    public MyTestMap(String dbPath, String dbName) {
        super(dbPath, dbName);
    }
    
    public MyTestMap(String dbPath, String dbName, int cacheSize) {
        super(dbPath, dbName, cacheSize);
    }
    
    public MyTestMap(String dbPath, String dbName, int cacheSize,
            int bloomFilterSize) {
        super(dbPath, dbName, cacheSize, bloomFilterSize);
    }
    
    @Override
    public void optimize() {
        try {
            this.initializeBloomFilter();
            for (Entry<Integer, Integer> entry : this.entrySet()) {
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
            Integer ki = (Integer) key;
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
        Integer ki = (Integer) key;
        if (bloomFilter.mightContain((Integer) key)) {
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
    public Integer put(Integer key, Integer value) {
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
        if (this.size > 0 && this.bloomFilter.mightContain((Integer) key)) {
            v = this.get(key);
        }
        
        if (v != null) {
            byte[] fullKeyArr = keySerFunc.apply((Integer) key);
            db.delete(fullKeyArr);
            size--;
        }
        return v;
    }
    
    @Override
    public void putAll(Map<? extends Integer, ? extends Integer> m) {
        try {
            WriteBatch batch = db.createWriteBatch();
            int counter = 0;
            for (Map.Entry<? extends Integer, ? extends Integer> e : m
                    .entrySet()) {
                byte[] keyArr = keySerFunc.apply(e.getKey());
                Integer v = null;
                Integer k = e.getKey();
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
            Throwables.propagate(ex);
        }
        
    }
    
    @Override
    public void clear() {
        this.initialize();
        this.initializeBloomFilter();
    }
    
    /* Iterators and Collections based on this Map */
    @Override
    public Set<Integer> keySet() {
        return new MapKeySet<Integer>(this, keyDeSerFunc);
    }
    
    @Override
    public Collection<Integer> values() {
        return new ValueCollection<Integer>(this, this.getDB(),
                this.valDeSerFunc);
    }
    
    @Override
    public Set<java.util.Map.Entry<Integer, Integer>> entrySet() {
        return new MapEntrySet<Integer, Integer>(this, this.keyDeSerFunc,
                this.valDeSerFunc);
    }
    

    
    /* Serialization functions go here */
    private void writeObject(java.io.ObjectOutputStream stream)
            throws IOException {
        this.serialize(stream);
    }
    
    private void readObject(java.io.ObjectInputStream in) throws IOException,
            ClassNotFoundException {
        keySerFunc = new IntegerSerDe.IntegerSerFunction();
        valSerFunc = new IntegerSerDe.IntegerSerFunction();
        keyDeSerFunc = new IntegerSerDe.IntegerDeSerFunction();
        valDeSerFunc = new IntegerSerDe.IntegerDeSerFunction();
        this.deserialize(in);
    }
    /* End of Serialization functions go here */
    
}
