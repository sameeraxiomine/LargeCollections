/*
 * Copyright 2014 Sameer Wadkar
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

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import org.iq80.leveldb.DB;

import com.google.common.base.Function;

public  class ValueCollection<V> implements Collection<V> {
    private Map<?,V> map = null;
    private DB db = null;
    private Function<byte[],? extends V> valDeSerFunc = null; 
    public ValueCollection(Map<?,V> map, DB db, Function<byte[],? extends V> valDeSerFunc) {
        this.map = map;
        this.db = db;
        this.valDeSerFunc = valDeSerFunc;
    }

    public int size() {
        return this.map.size();
    }

    public boolean isEmpty() {
        return this.map.isEmpty();
    }

    public boolean contains(Object o) {
        // TODO Auto-generated method stub
        return this.map.containsKey(o);
    }

    public Iterator<V> iterator() {
        return new MapValueIterator(this.db,this.valDeSerFunc);
    }

    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    public <T> T[] toArray(T[] a) {
        throw new UnsupportedOperationException();
    }

    public boolean add(V e) {
        throw new UnsupportedOperationException();
    }

    public boolean remove(Object o) {
        return (this.map.remove(o) != null);
    }

    public boolean containsAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    public boolean addAll(Collection<? extends V> c) {
        throw new UnsupportedOperationException();
    }

    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
        /*
        boolean changed = false;
        for(Object key:c){
            V v = this.map.remove(key);
            if(v!=null){
                changed = true;
            }
        }
        return changed;
        */
    }

    /*Verify is deletes can be invoked by iterator*/
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
        /*  
        boolean changed = false;
        Set s = this.map.keySet();
        Iterator iter = s.iterator();
        while(iter.hasNext()){
            Object key = iter.next();
            if(!c.contains(key)){
                iter.remove();
                changed = true;
            }
        }
        return changed;
        */
    }

    public void clear() {
        this.map.clear();
    }

}