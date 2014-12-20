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
import com.axiomine.largecollections.serdes.WritableSerDes;

import org.apache.hadoop.io.*;

public class WritableKVMap extends LargeCollection implements   Map<Writable,Writable>, Serializable{
    public static final long               serialVersionUID = 2l;
    
    private transient Function<Writable, byte[]> keySerFunc  = new WritableSerDes.SerFunction();
    private transient Function<Writable, byte[]> valSerFunc  = new WritableSerDes.SerFunction();    
    private transient Function<byte[], ? extends Writable> keyDeSerFunc     = null;
    private transient Function<byte[], ? extends Writable> valDeSerFunc     = null;
    private String keyClass=null;
    private String valueClass=null;
    
    private static Function<byte[], ? extends Writable> getWritableDeSerFunction(String cls){
        Function<byte[], ? extends Writable> func = null;
        try{
            Writable cObj = (Writable) Class.forName(cls).newInstance();
            func = new WritableSerDes.DeSerFunction(cObj.getClass());

        }
        catch(Exception ex){
            throw Throwables.propagate(ex);
        }
        return func;        
    }
    
    public WritableKVMap(Class<? extends Writable> keyClass,Class<? extends Writable> valueClass) {
        super();
        this.keyClass = keyClass.getName();
        this.valueClass = valueClass.getName();
        this.keyDeSerFunc = getWritableDeSerFunction(this.keyClass);
        this.valDeSerFunc = getWritableDeSerFunction(this.valueClass);
    }
    
    public WritableKVMap(String dbName,Class<? extends Writable> keyClass,Class<? extends Writable> valueClass) {
        super(dbName);
        this.keyClass = keyClass.getName();
        this.valueClass = valueClass.getName();
        this.keyDeSerFunc = getWritableDeSerFunction(this.keyClass);
        this.valDeSerFunc = getWritableDeSerFunction(this.valueClass);

    }
    
    public WritableKVMap(String dbPath, String dbName,Class<? extends Writable> keyClass,Class<? extends Writable> valueClass) {
        super(dbPath, dbName);
        this.keyClass = keyClass.getName();
        this.valueClass = valueClass.getName();
        this.keyDeSerFunc = getWritableDeSerFunction(this.keyClass);
        this.valDeSerFunc = getWritableDeSerFunction(this.valueClass);

    }
    
    public WritableKVMap(String dbPath, String dbName, int cacheSize,Class<? extends Writable> keyClass,Class<? extends Writable> valueClass) {
        super(dbPath, dbName, cacheSize);
        this.keyClass = keyClass.getName();
        this.valueClass = valueClass.getName();
        this.keyDeSerFunc = getWritableDeSerFunction(this.keyClass);
        this.valDeSerFunc = getWritableDeSerFunction(this.valueClass);
    }
    
    public WritableKVMap(String dbPath, String dbName, int cacheSize,
            int bloomFilterSize,Class<? extends Writable> keyClass,Class<? extends Writable> valueClass) {
        super(dbPath, dbName, cacheSize, bloomFilterSize);
        this.keyClass = keyClass.getName();
        this.valueClass = valueClass.getName();
        this.keyDeSerFunc = getWritableDeSerFunction(this.keyClass);
        this.valDeSerFunc = getWritableDeSerFunction(this.valueClass);

    }
    
    @Override
    public void optimize() {
        try {
            this.initializeBloomFilter();
            for (Entry<Writable, Writable> entry : this.entrySet()) {
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
    public Writable get(Object key) {
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
    public Writable put(Writable key, Writable value) {
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
    public void putAll(Map<? extends Writable, ? extends Writable> m) {
        try {
            WriteBatch batch = db.createWriteBatch();
            int counter = 0;
            for (Map.Entry<? extends Writable, ? extends Writable> e : m
                    .entrySet()) {
                byte[] keyArr = keySerFunc.apply(e.getKey());
                Writable v = null;
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
    public Collection<Writable> values() {
        return new ValueCollection<Writable>(this, this.getDB(),
                this.valDeSerFunc);
    }
    
    @Override
    public Set<java.util.Map.Entry<Writable, Writable>> entrySet() {
        return new MapEntrySet<Writable, Writable>(this, this.keyDeSerFunc,
                this.valDeSerFunc);
    }
    

    
    /* Serialization functions go here */
    private void writeObject(java.io.ObjectOutputStream stream)
            throws IOException {
        this.serialize(stream);
        stream.writeObject(this.keyClass);
        stream.writeObject(this.valueClass);
    }
    
    private void readObject(java.io.ObjectInputStream in) throws IOException,
            ClassNotFoundException {

        this.deserialize(in);
        this.keyClass = (String)in.readObject();
        this.valueClass = (String)in.readObject();
        try{
            keySerFunc  = new WritableSerDes.SerFunction();
            valSerFunc  = new WritableSerDes.SerFunction();    
            this.keyDeSerFunc = getWritableDeSerFunction(this.keyClass);
            this.valDeSerFunc = getWritableDeSerFunction(this.valueClass);
        }
        catch(Exception ex){
            throw Throwables.propagate(ex);
        }
        
    }
    /* End of Serialization functions go here */
    
}
