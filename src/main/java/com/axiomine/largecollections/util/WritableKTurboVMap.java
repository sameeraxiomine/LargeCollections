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
package com.axiomine.largecollections.util;
import com.google.common.base.Throwables;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.iq80.leveldb.WriteBatch;

import com.google.common.base.Function;


import com.axiomine.largecollections.*;
import com.axiomine.largecollections.serdes.TurboDeSerializer;
import com.axiomine.largecollections.serdes.TurboSerializer;
import com.axiomine.largecollections.serdes.WritableSerDes;

import org.apache.hadoop.io.*;

public class WritableKTurboVMap<K extends Writable,V> extends LargeCollection implements   Map<Writable,V>, Serializable{
    public static final long   serialVersionUID = 2l;
    
    private transient TurboSerializer<Writable> keySerFunc  = new WritableSerDes.SerFunction();
    private transient TurboDeSerializer<? extends Writable> keyDeSerFunc     = null;
    
    private TurboSerializer<V> valSerFunc  = null;
    private TurboDeSerializer<? extends V> valDeSerFunc     = null;
    private Class<K> kClass = null;
    
    
    public WritableKTurboVMap(Class<K> keyClass,TurboSerializer<V> vSerializer,TurboDeSerializer<V> vDeSerializer) {
        super();
        this.kClass = keyClass;
        this.valSerFunc = vSerializer;
        this.valDeSerFunc = vDeSerializer;
        this.keyDeSerFunc = new WritableSerDes.DeSerFunction(this.kClass);
    }
    
    public WritableKTurboVMap(String dbName,Class<K> keyClass,TurboSerializer<V> vSerializer,TurboDeSerializer<V> vDeSerializer) {
        super(dbName);
        this.kClass = keyClass;
        this.valSerFunc = vSerializer;
        this.valDeSerFunc = vDeSerializer;
        this.keyDeSerFunc = new WritableSerDes.DeSerFunction(this.kClass);

    }
    
    public WritableKTurboVMap(String dbPath, String dbName,Class<K> keyClass,TurboSerializer<V> vSerializer,TurboDeSerializer<V> vDeSerializer) {
        super(dbPath, dbName);
        this.kClass = keyClass;
        this.valSerFunc = vSerializer;
        this.valDeSerFunc = vDeSerializer;
        this.keyDeSerFunc = new WritableSerDes.DeSerFunction(this.kClass);
    }
    
    public WritableKTurboVMap(String dbPath, String dbName, int cacheSize,Class<K> keyClass,TurboSerializer<V> vSerializer,TurboDeSerializer<V> vDeSerializer) {
        super(dbPath, dbName, cacheSize);
        this.kClass = keyClass;
        this.valSerFunc = vSerializer;
        this.valDeSerFunc = vDeSerializer;
        this.keyDeSerFunc = new WritableSerDes.DeSerFunction(this.kClass);
    }
    
    public WritableKTurboVMap(String dbPath, String dbName, int cacheSize,
            int bloomFilterSize,Class<K> keyClass,TurboSerializer<V> vSerializer,TurboDeSerializer<V> vDeSerializer) {
        super(dbPath, dbName, cacheSize, bloomFilterSize);
        this.kClass = keyClass;
        this.valSerFunc = vSerializer;
        this.valDeSerFunc = vDeSerializer;
        this.keyDeSerFunc = new WritableSerDes.DeSerFunction(this.kClass);
    }
    
    @Override
    public void optimize() {
        MapKeySet<Writable> keys = new MapKeySet<Writable>(this, keyDeSerFunc);
        try {
            this.initializeBloomFilter();
            for (Writable entry : keys) {
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
            Writable ki = (Writable) key;
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
    public V get(Object key) {
        byte[] vbytes = null;
        if (key == null) {
            return null;
        }
        Writable ki = (Writable) key;
        if (bloomFilter.mightContain((Writable) key)) {
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
    public V put(Writable key, V value) {
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
    public V remove(Object key) {
        V v = null;
        if (key == null)
            return v;
        if (this.size > 0 && this.bloomFilter.mightContain((Writable) key)) {
            v = this.get(key);
        }
        
        if (v != null) {
            byte[] fullKeyArr = keySerFunc.apply((Writable) key);
            db.delete(fullKeyArr);
            size--;
        }
        return v;
    }
    
    @Override
    public void putAll(Map<? extends Writable, ? extends V> m) {
        try {
            WriteBatch batch = db.createWriteBatch();
            int counter = 0;
            for (Map.Entry<? extends Writable, ? extends V> e : m
                    .entrySet()) {
                byte[] keyArr = keySerFunc.apply(e.getKey());
                V v = null;
                Writable k = e.getKey();
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
    public Set<Writable> keySet() {
        return new MapKeySet<Writable>(this, keyDeSerFunc);
    }
    
    @Override
    public Collection<V> values() {
        return new ValueCollection<V>(this, this.getDB(),
                this.valDeSerFunc);
    }
    
    @Override
    public Set<java.util.Map.Entry<Writable, V>> entrySet() {
        return new MapEntrySet<Writable, V>(this, this.keyDeSerFunc,
                this.valDeSerFunc);
    }
    

    
    /* Serialization functions go here */
    private void writeObject(java.io.ObjectOutputStream stream)
            throws IOException {
        this.serialize(stream);
        stream.writeObject(this.kClass);
        stream.writeObject(this.valSerFunc);
        stream.writeObject(this.valDeSerFunc);
    }
    
    private void readObject(java.io.ObjectInputStream in) throws IOException,
            ClassNotFoundException {

        this.deserialize(in);
        this.kClass = (Class<K>)in.readObject();
        this.valSerFunc = (TurboSerializer<V> )in.readObject();
        this.valDeSerFunc = (TurboDeSerializer<? extends V> )in.readObject();
        this.keySerFunc  = new WritableSerDes.SerFunction();
        this.keyDeSerFunc = new WritableSerDes.DeSerFunction(this.kClass);
    }
    /* End of Serialization functions go here */
    
}

