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
package com.axiomine.largecollections;

import java.util.AbstractMap.SimpleEntry;
import java.util.Iterator;
import java.util.Map.Entry;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;

import com.google.common.base.Function;
import com.google.common.base.Throwables;

public final class MapEntryIterator<K, V> implements
        Iterator<java.util.Map.Entry<K, V>> {

    private DBIterator iter = null;
    private Function<byte[],? extends K> keyDeSerFunc = null;
    private Function<byte[],? extends V> valDeSerFunc = null;
    protected MapEntryIterator(DB db,Function<byte[],? extends K> keyDeSerFunc,Function<byte[],? extends V> valDeSerFunc) {
        try {
            this.keyDeSerFunc = keyDeSerFunc;
            this.valDeSerFunc = valDeSerFunc;
            this.iter = db.iterator();
            if (this.iter.hasPrev())
                this.iter.seekToLast();
            this.iter.seekToFirst();
        } catch (Exception ex) {
            Throwables.propagate(ex);
        }
    }

    public boolean hasNext() {
        boolean hasNext = iter.hasNext();
        return hasNext;
    }

    public java.util.Map.Entry<K, V> next() {
        Entry<byte[], byte[]> entry = this.iter.next();
        return new SimpleEntry(this.keyDeSerFunc.apply(entry.getKey()),
                               this.valDeSerFunc.apply(entry.getValue()));
    }

    public void remove() {
        this.iter.remove();
    }

}
