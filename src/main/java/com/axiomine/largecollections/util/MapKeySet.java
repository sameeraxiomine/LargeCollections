/*
 * Copyright 2015 Axomine LLC
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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;

import com.axiomine.largecollections.serdes.TurboDeSerializer;
import com.google.common.base.Function;
import com.google.common.base.Throwables;

public class MapKeySet<K> implements Set<K>, Closeable {
    private Map<K, ?>                      map                    = null;
    private DB                             db                     = null;
    private TurboDeSerializer<? extends K> deSerFunc              = null;
    private int                            DELETE_EVERY_N_RECORDS = 1000000;
    
    public MapKeySet(Map<K, ?> map, TurboDeSerializer<? extends K> deSerFunc) {
        this.map = map;
        this.db = ((IDb) this.map).getDB();
        this.deSerFunc = deSerFunc;
    }
    
    public int size() {
        return map.size();
    }
    
    public boolean isEmpty() {
        return map.isEmpty();
    }
    
    public boolean contains(Object o) {
        return map.containsKey(o);
    }
    
    public Iterator<K> iterator() {
        return new MapKeyIterator<K>((LargeCollection) this.map, this.deSerFunc);
    }
    
    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }
    
    public <T> T[] toArray(T[] a) {
        throw new UnsupportedOperationException();
    }
    
    public boolean add(K e) {
        throw new UnsupportedOperationException();
    }
    
    public boolean remove(Object o) {
        Object v = this.map.remove(o);
        return (v != null);
    }
    
    public void clear() {
        this.map.clear();
    }
    
    public boolean containsAll(Collection<?> c) {
        boolean containsAll = true;
        Iterator i = c.iterator();
        while (i.hasNext()) {
            Object key = i.next();
            if (!this.map.containsKey(key)) {
                containsAll = false;
                break;// Even if one element is not there return immediately
                      // with false
            }
        }
        return containsAll;
    }
    
    public boolean addAll(Collection<? extends K> c) {
        throw new UnsupportedOperationException();
    }
    
    public boolean retainAll(Collection<?> c) {
        boolean changed = false;
        List<K> keysToDelete = null;
        DBIterator dbIter = null;
        try {
            do {
                keysToDelete = new ArrayList<K>();
                dbIter = this.db.iterator();
                while (dbIter.hasNext()) {
                    Entry<byte[], byte[]> entry = dbIter.next();
                    Object o = deSerFunc.apply(entry.getKey());
                    if (!c.contains(o)) {
                        keysToDelete.add((K) o);
                        if (keysToDelete.size() > DELETE_EVERY_N_RECORDS) {
                            break;
                        }
                    }
                    
                }
                for (Object o : keysToDelete) {
                    if (this.map.remove(o) != null) {
                        changed = true;
                    }
                    
                }
            } while (keysToDelete.size() > 0);
            return changed;
            
        } finally {
            if (dbIter != null) {
                try {
                    dbIter.close();
                } catch (Exception ex) {
                    throw Throwables.propagate(ex);
                }
            }
        }
    }
    
    public boolean removeAll(Collection<?> c) {
        boolean removed = false;
        Iterator i = c.iterator();
        while (i.hasNext()) {
            Object key = i.next();
            if (this.map.remove(key) != null) {
                removed = true;
            }
        }
        return removed;
    }
    
    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub
        
    }
    
}
