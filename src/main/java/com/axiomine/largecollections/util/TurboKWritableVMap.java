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

public class TurboKWritableVMap<K, V extends Writable> extends LargeCollection
        implements Map<K, Writable>, Serializable {
    public static final long                                serialVersionUID = 2l;
    
    private transient TurboSerializer<K>                    keySerFunc       = null;
    private transient TurboDeSerializer<K>                  keyDeSerFunc     = null;
    private transient TurboSerializer<Writable>             valSerFunc       = new WritableSerDes.SerFunction();
    private transient TurboDeSerializer<? extends Writable> valDeSerFunc     = null;
    private Class<V>                                        vClass           = null;
    
    
    public TurboKWritableVMap(Class<V> valueClass,TurboSerializer<K> kSerializer, TurboDeSerializer<K> kDeSerializer) {
        super();
        this.vClass = valueClass;
        this.keySerFunc = kSerializer;
        this.keyDeSerFunc = kDeSerializer;
        this.valDeSerFunc = new WritableSerDes.DeSerFunction(this.vClass);
    }
    
    public TurboKWritableVMap(String dbName,
            Class<V> valueClass,TurboSerializer<K> kSerializer, TurboDeSerializer<K> kDeSerializer) {
        super(dbName);
        this.vClass = valueClass;
        this.keySerFunc = kSerializer;
        this.keyDeSerFunc = kDeSerializer;
        this.valDeSerFunc = new WritableSerDes.DeSerFunction(this.vClass);
    }
    
    public TurboKWritableVMap(String dbPath, String dbName,
                              Class<V> valueClass,TurboSerializer<K> kSerializer, TurboDeSerializer<K> kDeSerializer) {
        super(dbPath, dbName);
        this.vClass = valueClass;
        this.keySerFunc = kSerializer;
        this.keyDeSerFunc = kDeSerializer;
        this.valDeSerFunc = new WritableSerDes.DeSerFunction(this.vClass);
    }
    
    public TurboKWritableVMap(String dbPath, String dbName, int cacheSize,
                              Class<V> valueClass,TurboSerializer<K> kSerializer, TurboDeSerializer<K> kDeSerializer) {
        super(dbPath, dbName, cacheSize);
        this.vClass = valueClass;
        this.keySerFunc = kSerializer;
        this.keyDeSerFunc = kDeSerializer;
        this.valDeSerFunc = new WritableSerDes.DeSerFunction(this.vClass);
    }
    
    public TurboKWritableVMap(String dbPath, String dbName, int cacheSize,int bloomFilterSize, 
                              Class<V> valueClass,TurboSerializer<K> kSerializer, TurboDeSerializer<K> kDeSerializer) {
        super(dbPath, dbName, cacheSize, bloomFilterSize);
        this.vClass = valueClass;
        this.keySerFunc = kSerializer;
        this.keyDeSerFunc = kDeSerializer;
        this.valDeSerFunc = new WritableSerDes.DeSerFunction(this.vClass);
    }
    
    @Override
    public void optimize() {
        MapKeySet<K> keys = new MapKeySet<K>(this, keyDeSerFunc);
        try {
            this.initializeBloomFilter();
            for (K entry : keys) {
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
            K ki = (K) key;
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
    public Writable get(Object key) {
        byte[] vbytes = null;
        if (key == null) {
            return null;
        }
        K ki = (K) key;
        if (bloomFilter.mightContain((K) key)) {
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
    public Writable put(K key, Writable value) {
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
    public Writable remove(Object key) {
        Writable v = null;
        if (key == null)
            return v;
        if (this.size > 0 && this.bloomFilter.mightContain((K) key)) {
            v = this.get(key);
        }
        
        if (v != null) {
            byte[] fullKeyArr = keySerFunc.apply((K) key);
            db.delete(fullKeyArr);
            size--;
        }
        return v;
    }
    
    @Override
    public void putAll(Map<? extends K, ? extends Writable> m) {
        try {
            WriteBatch batch = db.createWriteBatch();
            int counter = 0;
            for (Map.Entry<? extends K, ? extends Writable> e : m.entrySet()) {
                byte[] keyArr = keySerFunc.apply(e.getKey());
                Writable v = null;
                K k = e.getKey();
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
    public Set<K> keySet() {
        return new MapKeySet<K>(this, keyDeSerFunc);
    }
    
    @Override
    public Collection<Writable> values() {
        return new ValueCollection<Writable>(this, this.getDB(),
                this.valDeSerFunc);
    }
    
    @Override
    public Set<java.util.Map.Entry<K, Writable>> entrySet() {
        return new MapEntrySet<K, Writable>(this, this.keyDeSerFunc,
                this.valDeSerFunc);
    }
    
    /* Serialization functions go here */
    private void writeObject(java.io.ObjectOutputStream stream)
            throws IOException {
        this.serialize(stream);
        stream.writeObject(this.vClass);
        stream.writeObject(this.keySerFunc);
        stream.writeObject(this.keyDeSerFunc);
    }
    
    private void readObject(java.io.ObjectInputStream in) throws IOException,
            ClassNotFoundException {
        this.deserialize(in);
        this.vClass = (Class<V>) in.readObject();
        this.keySerFunc = (TurboSerializer<K>) in.readObject();
        this.keyDeSerFunc = (TurboDeSerializer<K>) in.readObject();
        this.valSerFunc = new WritableSerDes.SerFunction();
        this.valDeSerFunc = new WritableSerDes.DeSerFunction(this.vClass);
    }
    /* End of Serialization functions go here */
    
}
