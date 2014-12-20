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

public class TurboKWritableVMap<K,V extends Writable> extends LargeCollection implements   Map<K,Writable>, Serializable{
    public static final long               serialVersionUID = 2l;
    
    private transient Function<K, byte[]> keySerFunc  = null;
    private transient Function<Writable, byte[]> valSerFunc  = new WritableSerDes.SerFunction();    
    private transient Function<byte[], ? extends K> keyDeSerFunc     = null;
    private transient Function<byte[], ? extends Writable> valDeSerFunc     = null;
    private String writableValClass=null;
    private String keySerCls=null;
    private String keyDeSerCls=null;

    
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
    
    public TurboKWritableVMap(Class<? extends Writable> valueClass,Function<K,byte[]> kSerializer,Function<byte[],K> kDeSerializer) {
        super();
        try{
            this.writableValClass = valueClass.getName();
            this.keySerFunc = kSerializer;
            this.keyDeSerFunc = kDeSerializer;
            this.keySerCls = kSerializer.getClass().getName();
            this.keyDeSerCls = kDeSerializer.getClass().getName();
            this.valDeSerFunc = getWritableDeSerFunction(this.writableValClass);
        }
        catch(Exception ex){
            throw Throwables.propagate(ex);
        }
    }
    
    public TurboKWritableVMap(String dbName,Class<? extends Writable> valueClass,Function<K,byte[]> kSerializer,Function<byte[],K> kDeSerializer) {
        super(dbName);
        try{
            this.writableValClass = valueClass.getName();
            this.keySerFunc = kSerializer;
            this.keyDeSerFunc = kDeSerializer;
            this.keySerCls = kSerializer.getClass().getName();
            this.keyDeSerCls = kDeSerializer.getClass().getName();
            this.valDeSerFunc = getWritableDeSerFunction(this.writableValClass);
        }
        catch(Exception ex){
            throw Throwables.propagate(ex);
        }
    }
    
    public TurboKWritableVMap(String dbPath, String dbName,Class<? extends Writable> valueClass,Function<K,byte[]> kSerializer,Function<byte[],K> kDeSerializer) {
        super(dbPath, dbName);
        try{
            this.writableValClass = valueClass.getName();
            this.keySerFunc = kSerializer;
            this.keyDeSerFunc = kDeSerializer;
            this.keySerCls = kSerializer.getClass().getName();
            this.keyDeSerCls = kDeSerializer.getClass().getName();
            this.valDeSerFunc = getWritableDeSerFunction(this.writableValClass);
        }
        catch(Exception ex){
            throw Throwables.propagate(ex);
        }
    }
    
    public TurboKWritableVMap(String dbPath, String dbName, int cacheSize,Class<? extends Writable> valueClass,Function<K,byte[]> kSerializer,Function<byte[],K> kDeSerializer) {
        super(dbPath, dbName, cacheSize);
        try{
            this.writableValClass = valueClass.getName();
            this.keySerFunc = kSerializer;
            this.keyDeSerFunc = kDeSerializer;
            this.keySerCls = kSerializer.getClass().getName();
            this.keyDeSerCls = kDeSerializer.getClass().getName();
            this.valDeSerFunc = getWritableDeSerFunction(this.writableValClass);
        }
        catch(Exception ex){
            throw Throwables.propagate(ex);
        }
    }
    
    public TurboKWritableVMap(String dbPath, String dbName, int cacheSize,
            int bloomFilterSize,Class<? extends Writable> valueClass,Function<K,byte[]> kSerializer,Function<byte[],K> kDeSerializer) {
        super(dbPath, dbName, cacheSize, bloomFilterSize);
        try{
            this.writableValClass = valueClass.getName();
            this.keySerFunc = kSerializer;
            this.keyDeSerFunc = kDeSerializer;
            this.keySerCls = kSerializer.getClass().getName();
            this.keyDeSerCls = kDeSerializer.getClass().getName();
            this.valDeSerFunc = getWritableDeSerFunction(this.writableValClass);
        }
        catch(Exception ex){
            throw Throwables.propagate(ex);
        }
    }
    
    @Override
    public void optimize() {
        try {
            this.initializeBloomFilter();
            for (Entry<K, Writable> entry : this.entrySet()) {
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
            for (Map.Entry<? extends K, ? extends Writable> e : m
                    .entrySet()) {
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
        stream.writeObject(this.writableValClass);
        stream.writeObject(this.keySerCls);
        stream.writeObject(this.keyDeSerCls);
    }
    
    private void readObject(java.io.ObjectInputStream in) throws IOException,
            ClassNotFoundException {

        this.deserialize(in);
        try{
            this.writableValClass = (String)in.readObject();
            this.keySerCls = (String)in.readObject();
            this.keyDeSerCls = (String)in.readObject();
            this.keySerFunc = (Function<K, byte[]>) Class.forName(this.keySerCls).newInstance();
            this.valSerFunc  = new WritableSerDes.SerFunction();
            this.keyDeSerFunc = (Function<byte[], K>) Class.forName(this.keyDeSerCls).newInstance();
            this.valDeSerFunc = getWritableDeSerFunction(this.writableValClass);
        }
        catch(Exception ex){
            throw Throwables.propagate(ex);
        }
        
    }
    /* End of Serialization functions go here */
    
}
