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

import org.apache.hadoop.io.*;
import com.axiomine.largecollections.serdes.*;
import com.axiomine.largecollections.kryo.serializers.*;

public class CharacterMapWritableMap extends LargeCollection implements   Map<Character,MapWritable>, Serializable{
    public static final long               serialVersionUID = 2l;
    private transient TurboSerializer<Character> keySerFunc       = new CharacterSerDes.SerFunction();
    private transient TurboSerializer<Writable> valSerFunc  = new WritableSerDes.SerFunction();
    private transient TurboDeSerializer<Character> keyDeSerFunc     = new CharacterSerDes.DeSerFunction();
    private transient TurboDeSerializer<MapWritable> valDeSerFunc     = new WritableSerDes.MapWritableDeSerFunction();
    
    public CharacterMapWritableMap() {
        super();
    }
    
    public CharacterMapWritableMap(String dbName) {
        super(dbName);
    }
    
    public CharacterMapWritableMap(String dbPath, String dbName) {
        super(dbPath, dbName);
    }
    
    public CharacterMapWritableMap(String dbPath, String dbName, int cacheSize) {
        super(dbPath, dbName, cacheSize);
    }
    
    public CharacterMapWritableMap(String dbPath, String dbName, int cacheSize,
            int bloomFilterSize) {
        super(dbPath, dbName, cacheSize, bloomFilterSize);
    }
    
    @Override
    public void optimize() {
        MapKeySet<Character> keys = new MapKeySet<Character>(this, keyDeSerFunc);
        try {
            this.initializeBloomFilter();
            for (Character entry : keys) {
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
            Character ki = (Character) key;
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
    public MapWritable get(Object key) {
        byte[] vbytes = null;
        if (key == null) {
            return null;
        }
        Character ki = (Character) key;
        if (bloomFilter.mightContain((Character) key)) {
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
    public MapWritable put(Character key, MapWritable value) {
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
    public MapWritable remove(Object key) {
        MapWritable v = null;
        if (key == null)
            return v;
        if (this.size > 0 && this.bloomFilter.mightContain((Character) key)) {
            v = this.get(key);
        }
        
        if (v != null) {
            byte[] fullKeyArr = keySerFunc.apply((Character) key);
            db.delete(fullKeyArr);
            size--;
        }
        return v;
    }
    
    @Override
    public void putAll(Map<? extends Character, ? extends MapWritable> m) {
        try {
            WriteBatch batch = db.createWriteBatch();
            int counter = 0;
            for (Map.Entry<? extends Character, ? extends MapWritable> e : m
                    .entrySet()) {
                byte[] keyArr = keySerFunc.apply(e.getKey());
                MapWritable v = null;
                Character k = e.getKey();
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
    public Set<Character> keySet() {
        return new MapKeySet<Character>(this, keyDeSerFunc);
    }
    
    @Override
    public Collection<MapWritable> values() {
        return new ValueCollection<MapWritable>(this, this.getDB(),
                this.valDeSerFunc);
    }
    
    @Override
    public Set<java.util.Map.Entry<Character, MapWritable>> entrySet() {
        return new MapEntrySet<Character, MapWritable>(this, this.keyDeSerFunc,
                this.valDeSerFunc);
    }
    

    
    /* Serialization functions go here */
    private void writeObject(java.io.ObjectOutputStream stream)
            throws IOException {
        this.serialize(stream);
    }
    
    private void readObject(java.io.ObjectInputStream in) throws IOException,
            ClassNotFoundException {
        keySerFunc       = new CharacterSerDes.SerFunction();
        valSerFunc  = new WritableSerDes.SerFunction();
        keyDeSerFunc     = new CharacterSerDes.DeSerFunction();
        valDeSerFunc     = new WritableSerDes.MapWritableDeSerFunction();
        this.deserialize(in);
    }
    /* End of Serialization functions go here */
    
}
